"""HTTP client for the instrument portal.

All interaction with the portal goes through this class so the manager can
treat network failures uniformly. No method ever resubmits an export form on
its own: submitting is exclusively the manager's decision.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote, urljoin

import httpx


class PortalError(Exception):
    """Base class for portal communication problems."""


class PortalUnreachable(PortalError):
    """Connection-level failure (DNS, connect, timeout, broken response)."""


class PortalRejected(PortalError):
    """The portal answered with an HTTP error status."""

    def __init__(self, status_code: int, body: str):
        super().__init__(f"portal returned HTTP {status_code}: {body[:200]}")
        self.status_code = status_code
        self.body = body


# Form switches the local demo portal understands. They are whitelisted so a
# caller cannot smuggle arbitrary form fields into the submission.
_ALLOWED_FORM_FLAGS = {"slow", "disconnect", "crash", "duplicate"}


class PortalClient:
    def __init__(self, base_url: str, timeout: float = 15.0):
        self.base_url = self._validate_base_url(base_url)
        self._timeout = timeout

    @staticmethod
    def _validate_base_url(url: str) -> str:
        parsed = httpx.URL(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("portal_url must start with http:// or https://")
        if not parsed.host:
            raise ValueError("portal_url must include a host")
        if parsed.query or parsed.fragment:
            raise ValueError("portal_url must not contain a query or fragment")
        # Keep a single trailing slash so urljoin behaves predictably.
        return str(parsed).rstrip("/") + "/"

    def _url(self, path: str) -> str:
        return urljoin(self.base_url, path.lstrip("/"))

    @staticmethod
    def build_form(batch_no: str, options: dict | None = None) -> dict[str, str]:
        form: dict[str, str] = {"batch_no": batch_no}
        for key, value in (options or {}).items():
            if key in _ALLOWED_FORM_FLAGS and value:
                form[key] = "true"
        return form

    async def submit_export(self, form: dict[str, str]) -> str:
        """POST the export form. Returns the portal task id on a full response.

        Raises PortalUnreachable when the connection is accepted but the
        response is lost (the manager must then reconcile via the task table).
        """
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(self._url("/export"), data=form)
        except httpx.HTTPError as exc:
            # ConnectError, TimeoutException, RemoteProtocolError, ...
            raise PortalUnreachable(str(exc)) from exc
        if resp.status_code >= 400:
            raise PortalRejected(resp.status_code, resp.text)
        try:
            data = resp.json()
            task_id = data["task_id"]
        except (ValueError, KeyError) as exc:
            raise PortalUnreachable(
                f"portal response carried no task id: {resp.text[:200]}"
            ) from exc
        if not isinstance(task_id, str) or not task_id:
            raise PortalUnreachable("portal returned an empty task id")
        return task_id

    async def list_tasks(self, batch_no: str) -> list[dict]:
        """Read the portal task table, filtered to one batch number."""
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(
                    self._url("/tasks"),
                    params={"batch_no": batch_no},
                )
        except httpx.HTTPError as exc:
            raise PortalUnreachable(str(exc)) from exc
        if resp.status_code >= 400:
            raise PortalRejected(resp.status_code, resp.text)
        tasks = resp.json()
        if not isinstance(tasks, list):
            raise PortalUnreachable("portal task table is not a JSON array")
        return tasks

    async def get_task(self, task_id: str) -> dict:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(
                    self._url(f"/tasks/{quote(task_id, safe='')}")
                )
        except httpx.HTTPError as exc:
            raise PortalUnreachable(str(exc)) from exc
        if resp.status_code >= 400:
            raise PortalRejected(resp.status_code, resp.text)
        return resp.json()

    async def download(
        self,
        task_id: str,
        tmp_dir: Path,
        expected_name: str,
    ) -> tuple[Path, str, int]:
        """Stream the result of the ORIGINAL task into a temporary file.

        The file is only returned once the full body has been received and
        matches the advertised Content-Length; callers verify the checksum and
        only then publish it under its final name.
        """
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / expected_name
        header_length: int | None = None
        content_type = "application/octet-stream"
        written = 0
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                async with client.stream(
                    "GET", self._url(f"/download/{quote(task_id, safe='')}")
                ) as resp:
                    if resp.status_code >= 400:
                        body = await resp.aread()
                        raise PortalRejected(resp.status_code, body.decode(
                            "utf-8", "replace"))
                    content_type = resp.headers.get(
                        "content-type", content_type)
                    cl = resp.headers.get("content-length")
                    if cl is not None:
                        try:
                            header_length = int(cl)
                        except ValueError:
                            header_length = None
                    with tmp_path.open("wb") as fh:
                        async for chunk in resp.aiter_bytes():
                            if chunk:
                                fh.write(chunk)
                                written += len(chunk)
                                if header_length is not None and written > header_length:
                                    raise PortalUnreachable(
                                        "downloaded body exceeds Content-Length")
        except httpx.HTTPError as exc:
            tmp_path.unlink(missing_ok=True)
            raise PortalUnreachable(str(exc)) from exc

        if header_length is not None and written != header_length:
            tmp_path.unlink(missing_ok=True)
            raise PortalUnreachable(
                f"truncated download: got {written} of {header_length} bytes")
        if written == 0:
            tmp_path.unlink(missing_ok=True)
            raise PortalUnreachable("downloaded file is empty")
        return tmp_path, content_type, written
