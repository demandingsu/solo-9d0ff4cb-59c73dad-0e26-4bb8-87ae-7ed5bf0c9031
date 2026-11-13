"""Pydantic request/response models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    portal_url: str = Field(
        ..., description="Base URL of the instrument portal, e.g. "
                         "http://portal:8000/")
    batch_no: str = Field(..., min_length=1, max_length=128)
    # Demo switches forwarded to the local portal form.
    options: dict[str, bool] | None = Field(
        default=None,
        description="Optional portal form flags: slow, disconnect, crash, "
                    "duplicate")


class AdoptRequest(BaseModel):
    portal_task_id: str = Field(..., min_length=1)


class JobOut(BaseModel):
    id: str
    portal_url: str
    batch_no: str
    portal_task_id: str | None
    status: str
    review_code: str | None
    review_reason: str | None
    retries: int
    result_sha256: str | None
    result_size: int | None
    last_error: str | None
    created_at: str
    updated_at: str
    reused: bool = False
    download_ready: bool = False


class AttemptOut(BaseModel):
    id: int
    job_id: str
    phase: str
    outcome: str
    detail: str | None
    at: str


class HealthOut(BaseModel):
    status: str
    active_jobs: int
    resumed_on_startup: int


def job_to_out(job: dict[str, Any], reused: bool = False) -> JobOut:
    return JobOut(
        id=job["id"],
        portal_url=job["portal_url"],
        batch_no=job["batch_no"],
        portal_task_id=job["portal_task_id"],
        status=job["status"],
        review_code=job["review_code"],
        review_reason=job["review_reason"],
        retries=job["retries"],
        result_sha256=job["result_sha256"],
        result_size=job["result_size"],
        last_error=job["last_error"],
        created_at=job["created_at"],
        updated_at=job["updated_at"],
        reused=reused,
        download_ready=(
            job["status"] == "COMPLETED"
            and bool(job["result_path"])
            and bool(job["result_sha256"])
        ),
    )
