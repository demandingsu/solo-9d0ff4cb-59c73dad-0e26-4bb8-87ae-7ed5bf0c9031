"""Local instrument portal (no account required).

Simulates an instrument web portal that accepts CSV export requests through an
HTML form, exposes a task table for polling, and serves generated result files.

Demonstration switches (query parameters on POST /export):
  slow=true            generate the CSV slowly (poll /tasks to watch progress)
  disconnect=true      accept the job, then drop the HTTP connection without
                       responding (simulates "accepted, response lost")
  crash=true           kill the portal process mid-submission (simulates the
                       service restarting while the collector is submitting)
  duplicate=true       immediately create a second task for the same batch, so
                       the task table contains two matching rows
"""
from __future__ import annotations

import asyncio
import csv
import os
import signal
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

DATA_DIR = Path(os.environ.get("PORTAL_DATA", "/data"))
TASKS_DB = DATA_DIR / "portal_tasks.tsv"
FILES_DIR = DATA_DIR / "files"

# Lifecycle of one portal-side export task.
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"

_lock = threading.Lock()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _read_rows() -> list[dict[str, str]]:
    if not TASKS_DB.exists():
        return []
    with TASKS_DB.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _write_rows(rows: list[dict[str, str]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cols = ["task_id", "batch_no", "status", "progress", "filename",
            "error", "created_at", "updated_at"]
    tmp = TASKS_DB.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(TASKS_DB)


def _upsert(row: dict[str, str]) -> None:
    with _lock:
        rows = _read_rows()
        for i, existing in enumerate(rows):
            if existing["task_id"] == row["task_id"]:
                rows[i] = row
                break
        else:
            rows.append(row)
        _write_rows(rows)


def _new_task(batch_no: str) -> dict[str, str]:
    task_id = uuid.uuid4().hex[:12]
    now = _now()
    return {
        "task_id": task_id,
        "batch_no": batch_no,
        "status": STATUS_PENDING,
        "progress": "0",
        "filename": "",
        "error": "",
        "created_at": now,
        "updated_at": now,
    }


async def _generate(row: dict[str, str], slow: bool) -> None:
    """Pretend to run the instrument export and write a CSV file."""
    row["status"] = STATUS_RUNNING
    row["updated_at"] = _now()
    _upsert(row)

    total_steps = 6 if slow else 2
    try:
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        tmp_path = FILES_DIR / f"{row['task_id']}.csv.tmp"
        final_path = FILES_DIR / f"{row['task_id']}.csv"
        with tmp_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["sample_id", "batch_no", "analyte",
                             "result", "unit", "measured_at"])
            for step in range(1, total_steps + 1):
                await asyncio.sleep(1 if slow else 0.2)
                writer.writerow([
                    f"S{step:04d}", row["batch_no"], "pH",
                    f"{7.0 + step * 0.01:.2f}", "pH", _now(),
                ])
                fh.flush()
                os.fsync(fh.fileno())
                row["progress"] = str(round(step * 100 / total_steps))
                row["updated_at"] = _now()
                _upsert(row)
        # Atomic publish: the download endpoint only exposes the final name.
        tmp_path.replace(final_path)
        row["status"] = STATUS_DONE
        row["progress"] = "100"
        row["filename"] = final_path.name
        row["updated_at"] = _now()
        _upsert(row)
    except Exception as exc:  # pragma: no cover - defensive
        row["status"] = STATUS_FAILED
        row["error"] = f"generation failed: {exc}"
        row["updated_at"] = _now()
        _upsert(row)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    # Resume any work that was interrupted by a portal restart.
    for row in _read_rows():
        if row["status"] in (STATUS_PENDING, STATUS_RUNNING):
            row["status"] = STATUS_RUNNING
            _upsert(row)
            asyncio.create_task(_generate(row, slow=True))
    yield


class SilentDropMiddleware:
    """Pure-ASGI middleware reproducing "portal accepted, response lost".

    The endpoint runs normally and the accepted task is persisted, but when
    the submitted form contains ``disconnect=true`` the HTTP response is
    suppressed and the TCP connection is closed mid-response. Clients see an
    incomplete response (httpx raises RemoteProtocolError) and must recover by
    consulting the portal task table instead of resubmitting.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        chunks: list[bytes] = []
        silent = {"on": False}

        async def wrapping_receive():
            message = await receive()
            if message["type"] == "http.request":
                chunks.append(message.get("body", b""))
                if b"disconnect=true" in b"".join(chunks):
                    # Known before the endpoint's response starts, so every
                    # response message can be swallowed.
                    silent["on"] = True
            return message

        async def wrapping_send(message):
            if silent["on"]:
                # Swallow the endpoint's real response.
                return
            await send(message)

        try:
            await self.app(scope, wrapping_receive, wrapping_send)
        finally:
            if silent["on"]:
                # Start a response that promises a body, then never send it and
                # raise after response.start. Uvicorn closes the transport
                # (instead of emitting a 500), so every client observes an
                # incomplete/truncated response while the accepted task row is
                # already safely persisted.
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", b"128"),
                        (b"connection", b"close"),
                    ],
                })
                raise ConnectionResetError(
                    "simulated: connection closed by portal after acceptance")


app = FastAPI(title="Instrument Portal (local demo)", lifespan=lifespan)
app.add_middleware(SilentDropMiddleware)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!doctype html><html><head><meta charset="utf-8">
<title>Instrument Export Portal</title></head><body>
<h1>Instrument Batch CSV Export</h1>
<form method="post" action="/export">
  <label>Batch number <input name="batch_no" required></label><br>
  <label><input type="checkbox" name="slow" value="true"> slow generation</label><br>
  <label><input type="checkbox" name="disconnect" value="true">
    disconnect after accept</label><br>
  <label><input type="checkbox" name="crash" value="true">
    crash during submission</label><br>
  <label><input type="checkbox" name="duplicate" value="true">
    create duplicate task</label><br>
  <button type="submit">Submit export</button>
</form>
<p><a href="/tasks">Task table</a></p>
</body></html>"""


@app.post("/export")
async def create_export(
    batch_no: str = Form(...),
    slow: str = Form(""),
    disconnect: str = Form(""),
    crash: str = Form(""),
    duplicate: str = Form(""),
):
    # Persist the accepted task BEFORE any simulated failure, so a restart can
    # find it in the task table.
    row = _new_task(batch_no)
    _upsert(row)

    if crash:
        # Simulate the portal process dying while the request is in flight.
        await asyncio.sleep(0.5)
        os.kill(os.getpid(), signal.SIGKILL)
        return  # unreachable

    asyncio.create_task(_generate(row, slow=(slow == "true")))

    if duplicate == "true":
        dup = _new_task(batch_no)
        _upsert(dup)
        asyncio.create_task(_generate(dup, slow=(slow == "true")))

    if disconnect == "true":
        # The task above is already accepted and persisted. The
        # SilentDropMiddleware suppresses this response and closes the socket.
        return JSONResponse(
            {"task_id": row["task_id"], "status": row["status"]},
            headers={"X-Portal-Task-Id": row["task_id"]},
        )

    return JSONResponse(
        {"task_id": row["task_id"], "status": row["status"]},
        headers={"X-Portal-Task-Id": row["task_id"]},
    )


@app.get("/tasks")
def list_tasks(batch_no: str | None = None) -> list[dict[str, str]]:
    """The portal task table. Filter by batch_no when supplied."""
    rows = _read_rows()
    if batch_no is not None:
        rows = [r for r in rows if r["batch_no"] == batch_no]
    return rows


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict[str, str]:
    for row in _read_rows():
        if row["task_id"] == task_id:
            return row
    raise HTTPException(status_code=404, detail="task not found")


@app.get("/download/{task_id}")
def download(task_id: str):
    for row in _read_rows():
        if row["task_id"] == task_id:
            break
    else:
        raise HTTPException(status_code=404, detail="task not found")
    if row["status"] != STATUS_DONE or not row["filename"]:
        raise HTTPException(status_code=409,
                            detail=f"task is {row['status']}, file not ready")
    path = FILES_DIR / row["filename"]
    if not path.exists():
        raise HTTPException(status_code=410, detail="result file missing")
    data = path.read_bytes()
    return PlainTextResponse(
        data,
        media_type="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="batch-{row["batch_no"]}-{task_id}.csv"'},
    )
