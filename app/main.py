"""FastAPI application: chunked, resumable, hash-verified file uploads."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import re
import shutil
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from . import config, db, storage

log = logging.getLogger("chunked-upload")

SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
# One byte-range spec: "bytes=<start>-<end>", "<start>-" or "-<suffix>".
SINGLE_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$", re.IGNORECASE)
# starlette renamed its 416 constant (the old alias now warns); pin our own
# so the whole supported fastapi/starlette range stays warning-free.
HTTP_416_RANGE_NOT_SATISFIABLE = 416


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Auto-initialise storage on every boot; existing state is preserved,
    # so interrupted sessions survive restarts.
    config.ensure_dirs()
    db.init_db()
    storage.reconcile()
    # Recovery sweep: expire idle leases that elapsed while we were down.
    _reap_once()
    stop = threading.Event()
    reaper = threading.Thread(
        target=_reap_loop, args=(stop,), name="session-reaper", daemon=True
    )
    reaper.start()
    try:
        yield
    finally:
        stop.set()
        reaper.join(timeout=5)


def _reap_once() -> None:
    try:
        expired = storage.expire_idle_sessions()
        if expired:
            log.info("reaper expired %d idle session(s): %s", len(expired), expired)
    except Exception:  # never let housekeeping take the process down
        log.exception("idle-session reaping failed")


def _reap_loop(stop: threading.Event) -> None:
    """Background reaper: periodically expire sessions past their lease."""
    while not stop.wait(config.REAPER_INTERVAL_SECONDS):
        _reap_once()


app = FastAPI(title="Chunked Upload Service", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------- schemas


class CreateSessionRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255)
    total_size: int = Field(gt=0, description="Total file size in bytes")
    chunk_size: int = Field(gt=0, le=config.MAX_CHUNK_SIZE)
    file_sha256: str | None = Field(
        default=None, description="Optional SHA-256 of the whole file, checked at merge"
    )
    idle_timeout_seconds: int = Field(
        default=config.DEFAULT_IDLE_TIMEOUT_SECONDS,
        gt=0,
        le=config.MAX_IDLE_TIMEOUT_SECONDS,
        description=(
            "Idle lease in seconds (default 86400, max 604800); the session "
            "expires if no chunk upload or status query happens within it"
        ),
    )

    @field_validator("file_sha256")
    @classmethod
    def _check_digest(cls, value: str | None) -> str | None:
        if value is not None:
            if not SHA256_RE.match(value):
                raise ValueError("file_sha256 must be 64 hex characters")
            return value.lower()
        return None


class CreateArchiveRequest(BaseModel):
    session_ids: list[str] = Field(
        min_length=1,
        max_length=1000,
        description="Ordered, non-repeating ids of completed sessions to export",
    )

    @field_validator("session_ids")
    @classmethod
    def _ordered_unique_nonempty(cls, value: list[str]) -> list[str]:
        if any(not item.strip() for item in value):
            raise ValueError("session_ids must not contain empty values")
        if len(set(value)) != len(value):
            raise ValueError("session_ids must not contain duplicates")
        return value


# ---------------------------------------------------------------- helpers


def _session_or_410(session):
    """Map an expired session onto the canonical 410 response."""
    if session["status"] == "expired":
        raise HTTPException(
            status.HTTP_410_GONE,
            detail={
                "message": "upload session has expired and its chunks were removed",
                "expired_at": session["expired_at"],
            },
        )


def _get_session_or_404(session_id: str):
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
        return session


def _parse_single_range(header: str, size: int) -> tuple[int, int]:
    """Parse one `bytes` range into an absolute, inclusive [start, end] pair.

    Raises ValueError for everything that must be answered with 416:
    malformed syntax, multiple ranges, or ranges outside the file.
    """
    if "," in header:
        raise ValueError("multiple ranges are not supported")
    match = SINGLE_RANGE_RE.match(header.strip())
    if match is None:
        raise ValueError("malformed Range header")
    first, last = match.groups()
    if first:
        start = int(first)
        end = int(last) if last else size - 1
        if start >= size or start > end:
            raise ValueError("range not satisfiable")
        return start, min(end, size - 1)
    if not last:
        raise ValueError("malformed Range header")
    suffix = int(last)
    if suffix == 0:
        raise ValueError("range not satisfiable")
    return max(size - suffix, 0), size - 1


def _weak_etag_match(header: str, etag: str) -> bool:
    """If-None-Match comparison: weak, per RFC 7232 (`W/` prefixes and a
    `*` member both match the current validator)."""
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.startswith("W/"):
            candidate = candidate[2:].strip()
        if candidate == etag:
            return True
    return False


def _strong_etag_match(header: str, etag: str) -> bool:
    """If-Range comparison: strong, per RFC 7233. Weak tags never match,
    and HTTP dates never match because we don't emit Last-Modified."""
    return header.strip() == etag


# ---------------------------------------------------------------- endpoints


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/uploads", status_code=status.HTTP_201_CREATED)
def create_session(body: CreateSessionRequest) -> dict:
    """Create an upload session. Chunk count is derived from size/chunk_size."""
    total_chunks = math.ceil(body.total_size / body.chunk_size)
    session_id = uuid.uuid4().hex
    now = db.utcnow()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO sessions (id, filename, total_size, chunk_size, "
            "total_chunks, file_sha256, status, idle_timeout_seconds, "
            "last_activity, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'uploading', ?, ?, ?, ?)",
            (
                session_id,
                body.filename,
                body.total_size,
                body.chunk_size,
                total_chunks,
                body.file_sha256,
                body.idle_timeout_seconds,
                now,
                now,
                now,
            ),
        )
        session = db.get_session(conn, session_id)
        return db.session_payload(conn, session)


@app.get("/uploads")
def list_sessions() -> dict:
    """List live (resumable/completed) sessions, newest first.

    Expired sessions stay readable via their own URL but are not advertised
    here as something clients can continue.
    """
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT s.*, "
            "       (SELECT COUNT(*) FROM chunks c WHERE c.session_id = s.id) "
            "       AS received "
            "FROM sessions s "
            "WHERE s.status != 'expired' "
            "ORDER BY s.created_at DESC LIMIT 200"
        ).fetchall()
    return {
        "sessions": [
            {
                "session_id": row["id"],
                "filename": row["filename"],
                "status": row["status"],
                "total_chunks": row["total_chunks"],
                "received_count": row["received"],
                "idle_timeout_seconds": row["idle_timeout_seconds"],
                "last_activity": row["last_activity"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
    }


@app.get("/uploads/{session_id}")
def get_session(session_id: str) -> dict:
    """Session status, including the list of missing chunk indexes.

    Expired sessions remain readable (status `expired`, with `expired_at`) but
    can no longer be resumed. A query against an uploading session counts as
    activity and refreshes its idle lease.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
        if session["status"] == "uploading":
            db.touch_session(conn, session_id, db.utcnow())
            conn.commit()
            session = db.get_session(conn, session_id)
        return db.session_payload(conn, session)


@app.put("/uploads/{session_id}/chunks/{index}")
async def upload_chunk(
    session_id: str,
    index: int,
    request: Request,
    x_chunk_sha256: str | None = Header(default=None),
    sha256: str | None = Query(default=None),
) -> dict:
    """Receive one chunk (raw body). Out-of-order and repeat uploads are fine.

    The chunk's SHA-256 must be supplied via the X-Chunk-SHA256 header or the
    ?sha256= query parameter; mismatched or wrongly sized payloads are rejected
    and nothing is persisted, so clients can simply retry.
    """
    digest = (x_chunk_sha256 or sha256 or "").lower()
    if not SHA256_RE.match(digest):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "provide the chunk SHA-256 via X-Chunk-SHA256 header or ?sha256=",
        )

    with db.connect() as conn:
        session = db.get_session(conn, session_id)
    if session is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    _session_or_410(session)
    if session["status"] != "uploading":
        raise HTTPException(status.HTTP_409_CONFLICT, "session already completed")
    if not 0 <= index < session["total_chunks"]:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"chunk index must be in [0, {session['total_chunks'] - 1}]",
        )

    expected_size = storage.expected_chunk_size(session, index)
    content_length = request.headers.get("content-length")
    if content_length is not None and int(content_length) != expected_size:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"chunk {index} must be exactly {expected_size} bytes",
        )

    # Stream to a temp file while hashing; publish atomically on success.
    cdir = storage.chunks_dir(session_id)
    cdir.mkdir(parents=True, exist_ok=True)
    tmp_path = cdir / f"{index:08d}.part.tmp-{uuid.uuid4().hex}"
    hasher = hashlib.sha256()
    size = 0
    try:
        with open(tmp_path, "wb") as out:
            async for piece in request.stream():
                hasher.update(piece)
                out.write(piece)
                size += len(piece)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    if size != expected_size:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"chunk {index} must be exactly {expected_size} bytes, got {size}",
        )
    if hasher.hexdigest() != digest:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "chunk sha256 mismatch")

    os.replace(tmp_path, storage.chunk_path(session_id, index))

    moment = db.utcnow()
    try:
        with db.connect() as conn:
            conn.execute(
                "INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (session_id, chunk_index) DO UPDATE SET "
                "size = excluded.size, sha256 = excluded.sha256, "
                "received_at = excluded.received_at",
                (session_id, index, size, digest, moment),
            )
            # A successful chunk upload renews the lease. The status guard is
            # the race boundary with the reaper: if it expires this session
            # concurrently (and deletes the just-inserted chunk row), the
            # UPDATE matches nothing and we must not return success.
            cur = conn.execute(
                "UPDATE sessions SET updated_at = ?, last_activity = ? "
                "WHERE id = ? AND status = 'uploading'",
                (moment, moment, session_id),
            )
            if cur.rowcount != 1:
                conn.rollback()
                storage.chunk_path(session_id, index).unlink(missing_ok=True)
                session = db.get_session(conn, session_id)
                raise HTTPException(
                    status.HTTP_410_GONE,
                    detail={
                        "message": "upload session has expired and its chunks were removed",
                        "expired_at": session["expired_at"] if session else None,
                    },
                )
            session = db.get_session(conn, session_id)
            payload = db.session_payload(conn, session)
    except HTTPException:
        raise
    except BaseException:
        storage.chunk_path(session_id, index).unlink(missing_ok=True)
        raise
    return payload


@app.post("/uploads/{session_id}/complete")
def complete_session(session_id: str) -> dict:
    """Merge all chunks into the final file. Idempotent.

    Fails with 409 (and the missing list) until every chunk is present; each
    chunk's hash is re-verified during the merge, and the target file is
    published atomically via rename. A session whose idle lease has expired
    is gone for good: the response is 410 with `expired_at`.
    """
    lock = storage.merge_lock(session_id)
    if not lock.acquire(blocking=False):
        raise HTTPException(
            status.HTTP_409_CONFLICT, "a merge is already in progress for this session"
        )
    try:
        try:
            return storage.merge_session(session_id)
        except KeyError:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
        except storage.SessionExpired as exc:
            raise HTTPException(
                status.HTTP_410_GONE,
                detail={
                    "message": "upload session has expired and its chunks were removed",
                    "expired_at": exc.expired_at,
                },
            )
        except storage.MergeError as exc:
            code = (
                status.HTTP_409_CONFLICT
                if exc.missing
                else status.HTTP_422_UNPROCESSABLE_ENTITY
            )
            raise HTTPException(
                code,
                detail={"message": str(exc), "missing_chunks": exc.missing},
            )
    finally:
        lock.release()


@app.get("/uploads/{session_id}/content")
def download_content(
    session_id: str,
    range_header: str | None = Header(default=None, alias="Range"),
    if_none_match: str | None = Header(default=None),
    if_range: str | None = Header(default=None),
):
    """Download the merged file (200) or a single byte range (206).

    The ETag is a strong validator built from the file's SHA-256, fixed at
    merge time. `If-None-Match` hits are answered 304; a non-matching
    `If-Range` makes us ignore `Range` and send the full file. Multi-range
    requests, malformed syntax and out-of-bounds ranges get 416 with
    `Content-Range: bytes */<size>`. The payload is streamed from disk in
    bounded blocks, never loaded into memory whole.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
    if session is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if session["status"] != "completed" or not session["final_path"]:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "session is not completed; upload all chunks and POST /complete first",
        )
    path = Path(session["final_path"])
    if not path.is_file():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "merged file is missing on disk")

    size = path.stat().st_size
    etag = f'"{session["file_sha256"]}"'
    cache_headers = {"ETag": etag, "Accept-Ranges": "bytes"}

    if if_none_match is not None and _weak_etag_match(if_none_match, etag):
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=cache_headers)

    start, end = 0, size - 1
    partial = False
    if range_header is not None and (
        if_range is None or _strong_etag_match(if_range, etag)
    ):
        try:
            start, end = _parse_single_range(range_header, size)
            partial = True
        except ValueError as exc:
            raise HTTPException(
                HTTP_416_RANGE_NOT_SATISFIABLE,
                detail=str(exc),
                headers={**cache_headers, "Content-Range": f"bytes */{size}"},
            )

    headers = {**cache_headers, "Content-Length": str(end - start + 1)}
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(
        storage.iter_file_range(path, start, end),
        status_code=status.HTTP_206_PARTIAL_CONTENT if partial else status.HTTP_200_OK,
        headers=headers,
        media_type="application/octet-stream",
    )


@app.delete("/uploads/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def abort_session(session_id: str):
    """Abort a session: delete its chunks, metadata and (if any) merged file.

    No `-> None` return annotation: fastapi < 0.117 mistakes it for a response
    model and refuses to boot ("Status code 204 must not have a response
    body"), which would take down the whole service on the oldest supported
    dependency set.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    shutil.rmtree(storage.session_dir(session_id), ignore_errors=True)
    if session["final_path"]:
        try:
            os.unlink(session["final_path"])
        except OSError:
            pass


# ---------------------------------------------------------------- archives


@app.post("/archives", status_code=status.HTTP_201_CREATED)
def create_archive(body: CreateArchiveRequest) -> dict:
    """Export several completed uploads as one ZIP, in the requested order.

    Every id must reference a completed session whose product still exists on
    disk; all source files are opened and pinned before the archive is written
    (bounded buffering), atomically published and recorded. Duplicate or empty
    ids are rejected at validation (422); an unknown session gives 404 and any
    non-completed / missing source gives 409, and in every error case no
    archive file or record is left behind.
    """
    try:
        return storage.create_archive(body.session_ids)
    except storage.ArchiveSourceError as exc:
        if exc.kind == "not_found":
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                detail={"message": "session not found", "session_id": exc.session_id},
            )
        if exc.kind == "not_completed":
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail={
                    "message": "session is not completed",
                    "session_id": exc.session_id,
                },
            )
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "message": "completed file is missing on disk",
                "session_id": exc.session_id,
            },
        )


@app.get("/archives/{archive_id}/content")
def download_archive(archive_id: str):
    """Download a previously created ZIP archive."""
    with db.connect() as conn:
        archive = db.get_archive(conn, archive_id)
    if archive is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "archive not found")
    path = Path(archive["path"])
    if not path.is_file():
        # Record without bytes should never happen (publish precedes the row);
        # answer honestly rather than streaming a 500 from FileResponse.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "archive file is missing on disk")
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{archive_id}.zip",
    )


@app.delete("/archives/{archive_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_archive(archive_id: str):
    """Delete an archive's ZIP and its record. Source sessions are untouched."""
    if not storage.delete_archive(archive_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "archive not found")
