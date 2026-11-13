"""FastAPI entrypoint for the LIMS export-collection API."""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .config import settings
from .database import Database
from .manager import JobManager
from .portal_client import PortalError, PortalUnreachable
from .schemas import AdoptRequest, HealthOut, JobCreate, JobOut, AttemptOut, job_to_out

db = Database(settings.db_path)
manager = JobManager(db, settings)
resumed = {"count": 0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    # Durable state drives recovery: resume every unfinished job after restart.
    resumed["count"] = await manager.resume_on_startup()
    yield
    await manager.shutdown()
    await db.close()


app = FastAPI(
    title="LIMS Export Collection API",
    version="1.0.0",
    description="Submits instrument-portal CSV exports, reconciles lost "
                "submissions via the portal task table, and serves verified "
                "result files.",
    lifespan=lifespan,
)


@app.post("/jobs", response_model=JobOut, status_code=201)
async def create_job(payload: JobCreate):
    try:
        job, reused = await manager.get_or_create_job(
            payload.portal_url, payload.batch_no, payload.options)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return job_to_out(job, reused=reused)


@app.get("/jobs", response_model=list[JobOut])
async def list_jobs(status: str | None = Query(default=None)):
    rows = await db.list_jobs(status=status)
    return [job_to_out(row) for row in rows]


@app.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: str):
    job = await db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job_to_out(job)


@app.get("/jobs/{job_id}/attempts", response_model=list[AttemptOut])
async def get_attempts(job_id: str):
    job = await db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return await db.list_attempts(job_id)


@app.post("/jobs/{job_id}/adopt", response_model=JobOut)
async def adopt_job(job_id: str, payload: AdoptRequest):
    job = await db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    try:
        updated = await manager.adopt_task(job_id, payload.portal_task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except PortalUnreachable as exc:
        raise HTTPException(status_code=502, detail=f"portal unreachable: {exc}")
    except PortalError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return job_to_out(updated)


@app.get("/jobs/{job_id}/download")
async def download_job(job_id: str):
    job = await db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] != "COMPLETED" or not job["result_path"]:
        raise HTTPException(
            status_code=409,
            detail=f"result file is not ready (job status: {job['status']})")

    path = Path(job["result_path"])
    if not path.is_file():
        # Durable row says complete but the file vanished: block download.
        raise HTTPException(
            status_code=410,
            detail="recorded result file is missing; request a re-download")

    # Re-verify integrity on every download; never serve a tampered/incomplete
    # file even if the database row looks healthy.
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != job["result_sha256"]:
        raise HTTPException(
            status_code=409,
            detail="stored file fails checksum verification; refusing download")
    size = path.stat().st_size
    if size != job["result_size"]:
        raise HTTPException(
            status_code=409,
            detail="stored file size does not match the recorded download")

    return FileResponse(
        path,
        media_type=job["content_type"] or "text/csv",
        filename=f"batch-{job['batch_no']}-{job['portal_task_id']}.csv",
    )


@app.get("/health", response_model=HealthOut)
async def health():
    active = await db.list_active_jobs()
    return HealthOut(
        status="ok",
        active_jobs=len(active),
        resumed_on_startup=resumed["count"],
    )
