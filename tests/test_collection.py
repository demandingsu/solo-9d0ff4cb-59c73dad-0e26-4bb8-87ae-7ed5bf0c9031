"""End-to-end behavior tests for the collection API against the local portal.

Covers the safety contract:
* happy path with slow generation and verified download
* concurrent / duplicate calls share one job
* accept-then-disconnect reconciles via the task table, no resubmission
* duplicate / missing portal tasks go to MANUAL_REVIEW, never resubmitted
* jobs resume from SQLite after a collector restart
* download is unavailable until a complete, verified file exists
"""
from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "collector"))

pytestmark = pytest.mark.asyncio


async def _wait_status(client: httpx.AsyncClient, job_id: str,
                       targets: set[str], timeout: float = 20.0) -> dict:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        resp = await client.get(f"/jobs/{job_id}")
        assert resp.status_code == 200
        job = resp.json()
        if job["status"] in targets:
            return job
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"job {job_id} stuck in {job['status']}, "
                f"wanted one of {targets}")
        await asyncio.sleep(0.2)


async def test_happy_path_slow_generation_and_verified_download(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c:
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-SLOW",
            "options": {"slow": True},
        })
        assert resp.status_code == 201
        job = resp.json()
        assert job["status"] in ("SUBMITTING", "POLLING")
        assert job["reused"] is False

        final = await _wait_status(c, job["id"], {"COMPLETED"})
        assert final["download_ready"] is True
        assert final["portal_task_id"]
        assert final["result_size"] and final["result_sha256"]

        dl = await c.get(f"/jobs/{job['id']}/download")
        assert dl.status_code == 200
        assert dl.headers["content-type"].startswith("text/csv")
        body = dl.content
        assert b"B-SLOW" in body
        assert hashlib.sha256(body).hexdigest() == final["result_sha256"]
        assert len(body) == final["result_size"]


async def test_concurrent_and_duplicate_calls_share_one_job(portal, collector):
    payload = {"portal_url": portal.base_url, "batch_no": "B-SHARED"}
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c:
        results = await asyncio.gather(*[
            c.post("/jobs", json=payload) for _ in range(5)
        ])
        ids = {r.json()["id"] for r in results}
        assert len(ids) == 1
        # Trailing slash / casing of the URL must not create a second job.
        again = await c.post("/jobs", json={
            "portal_url": portal.base_url + "/", "batch_no": "B-SHARED"})
        assert again.json()["id"] == ids.pop()
        assert again.json()["reused"] is True

        listed = await c.get("/jobs")
        matching = [j for j in listed.json() if j["batch_no"] == "B-SHARED"]
        assert len(matching) == 1


async def test_disconnect_reconciles_without_resubmission(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c, \
            httpx.AsyncClient(base_url=portal.base_url, timeout=10) as p:
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-DISC",
            "options": {"disconnect": True},
        })
        job_id = resp.json()["id"]

        final = await _wait_status(c, job_id, {"COMPLETED", "MANUAL_REVIEW"})
        assert final["status"] == "COMPLETED"

        # Exactly one portal-side task: the form was submitted only once.
        tasks = (await p.get("/tasks", params={"batch_no": "B-DISC"})).json()
        assert len(tasks) == 1
        assert final["portal_task_id"] == tasks[0]["task_id"]

        attempts = (await c.get(f"/jobs/{job_id}/attempts")).json()
        submit_starts = [a for a in attempts
                         if a["phase"] == "submit" and a["outcome"] == "start"]
        assert len(submit_starts) == 1
        assert any(a["phase"] == "reconcile" and a["outcome"] == "adopted"
                   for a in attempts)


async def test_duplicate_portal_tasks_go_to_manual_review(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c, \
            httpx.AsyncClient(base_url=portal.base_url, timeout=10) as p:
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-DUP",
            "options": {"disconnect": True, "duplicate": True},
        })
        job_id = resp.json()["id"]
        final = await _wait_status(c, job_id, {"MANUAL_REVIEW"})
        assert final["review_code"] == "DUPLICATE_TASKS"

        # No further submission: still exactly the two portal tasks.
        tasks = (await p.get("/tasks", params={"batch_no": "B-DUP"})).json()
        assert len(tasks) == 2

        # A blocked job must not offer a download.
        dl = await c.get(f"/jobs/{job_id}/download")
        assert dl.status_code == 409


async def test_no_portal_task_goes_to_manual_review(portal, tmp_path):
    # Exercise the not-found reconciliation branch in-process against a portal
    # that has no row for the batch. A synthetic RECONCILING row is inserted
    # directly, so no background poll/download task can race this test.
    from app.config import Settings
    from app.database import Database, RECONCILING, utcnow
    from app.manager import JobManager
    import uuid

    workdir = tmp_path / "nf"
    results = workdir / "results"
    tmpdir = workdir / "tmp"
    results.mkdir(parents=True)
    tmpdir.mkdir(parents=True)
    settings = Settings(
        db_path=str(workdir / "collector.db"),
        result_dir=str(results), tmp_dir=str(tmpdir),
        reconcile_max_attempts=3, reconcile_backoff=0.1)
    db = Database(settings.db_path)
    await db.connect()
    job_id = uuid.uuid4().hex
    await db.insert_job({
        "id": job_id,
        "portal_url": portal.base_url + "/",
        "batch_no": "B-GHOST",
        "portal_task_id": None,
        "status": RECONCILING,
        "submit_payload": {"batch_no": "B-GHOST"},
        "created_at": utcnow(),
        "updated_at": utcnow(),
    })

    manager = JobManager(db, settings)
    await manager._reconcile(job_id)
    row = await db.get_job(job_id)
    assert row["status"] == "MANUAL_REVIEW"
    assert row["review_code"] == "TASK_NOT_FOUND"

    attempts = await db.list_attempts(job_id)
    # Reconciliation only queries the task table; nothing is resubmitted.
    assert all(a["phase"] != "submit" for a in attempts)
    await db.close()


async def test_job_resumes_after_collector_restart(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c:
        # Slow generation keeps the job active across the collector restart.
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-RESTART",
            "options": {"slow": True},
        })
        job_id = resp.json()["id"]
        await _wait_status(c, job_id, {"POLLING"})

    # Restart the collector process against the same SQLite file.
    collector.restart()

    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c:
        health = (await c.get("/health")).json()
        assert health["resumed_on_startup"] >= 1
        final = await _wait_status(c, job_id, {"COMPLETED", "MANUAL_REVIEW"})
        assert final["status"] == "COMPLETED"
        dl = await c.get(f"/jobs/{job_id}/download")
        assert dl.status_code == 200
        assert hashlib.sha256(dl.content).hexdigest() == final["result_sha256"]


async def test_download_blocked_until_complete(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c:
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-BLOCK",
            "options": {"slow": True},
        })
        job_id = resp.json()["id"]
        # Immediately after creation the file must not be downloadable.
        job = (await c.get(f"/jobs/{job_id}")).json()
        if job["status"] != "COMPLETED":
            dl = await c.get(f"/jobs/{job_id}/download")
            assert dl.status_code == 409
        await _wait_status(c, job_id, {"COMPLETED"})
        dl = await c.get(f"/jobs/{job_id}/download")
        assert dl.status_code == 200


async def test_manual_adopt_resumes_a_duplicate_review_job(portal, collector):
    async with httpx.AsyncClient(base_url=collector.base_url, timeout=10) as c, \
            httpx.AsyncClient(base_url=portal.base_url, timeout=10) as p:
        resp = await c.post("/jobs", json={
            "portal_url": portal.base_url,
            "batch_no": "B-ADOPT",
            "options": {"disconnect": True, "duplicate": True},
        })
        job_id = resp.json()["id"]
        await _wait_status(c, job_id, {"MANUAL_REVIEW"})

        tasks = (await p.get("/tasks", params={"batch_no": "B-ADOPT"})).json()
        chosen = sorted(t["task_id"] for t in tasks)[0]
        adopted = await c.post(f"/jobs/{job_id}/adopt",
                               json={"portal_task_id": chosen})
        assert adopted.status_code == 200
        final = await _wait_status(c, job_id, {"COMPLETED"})
        assert final["portal_task_id"] == chosen
