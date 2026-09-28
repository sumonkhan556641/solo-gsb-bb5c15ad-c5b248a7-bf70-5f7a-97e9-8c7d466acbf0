"""On-disk layout and merge logic for chunk payloads."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import sqlite3
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from . import config, db

_COPY_BUFFER = 1024 * 1024  # 1 MiB streaming buffer for merge/verify

log = logging.getLogger("chunked-upload")

# Guards against concurrent merges of the same session. The idle-session
# reaper also takes (non-blocking) this lock, so a session mid-merge can
# never be expired underneath it.
_merge_locks: dict[str, threading.Lock] = {}
_merge_locks_guard = threading.Lock()


def merge_lock(session_id: str) -> threading.Lock:
    with _merge_locks_guard:
        return _merge_locks.setdefault(session_id, threading.Lock())


class SessionExpired(Exception):
    """Raised when an operation targets an expired session.

    Carries the recorded expiry timestamp for the 410 response body.
    """

    def __init__(self, expired_at: str | None):
        super().__init__("upload session has expired")
        self.expired_at = expired_at


def sanitize_filename(name: str) -> str:
    """Reduce a client-supplied filename to a safe path component."""
    base = os.path.basename(name.strip().replace(" ", "_"))
    return re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._") or "file"


def session_dir(session_id: str) -> Path:
    return config.sessions_dir() / session_id


def chunks_dir(session_id: str) -> Path:
    return session_dir(session_id) / "chunks"


def chunk_path(session_id: str, index: int) -> Path:
    return chunks_dir(session_id) / f"{index:08d}.part"


def expected_chunk_size(session: sqlite3.Row, index: int) -> int:
    """All chunks are `chunk_size` bytes except the last, which takes the remainder."""
    if index == session["total_chunks"] - 1:
        return session["total_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
    return session["chunk_size"]


def final_path_for(session: sqlite3.Row) -> Path:
    name = f"{session['id']}_{sanitize_filename(session['filename'])}"
    return config.files_dir() / name


def iter_file_range(path: Path, start: int, end: int) -> Iterator[bytes]:
    """Yield the inclusive byte range [start, end] of `path` in bounded blocks.

    Reads are capped at _COPY_BUFFER, so even a huge range is streamed
    without ever loading the whole file into memory.
    """
    remaining = end - start + 1
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(_COPY_BUFFER, remaining))
            if not block:
                break  # file shrank underneath us; stop cleanly
            remaining -= len(block)
            yield block


def reconcile() -> None:
    """Drop chunk rows whose payload file vanished (crash between DB and disk).

    Runs at startup so a restarted service reports exactly the chunks that are
    physically present; clients simply re-upload the rest.
    """
    with db.connect() as conn:
        sessions = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        for session in sessions:
            rows = conn.execute(
                "SELECT chunk_index, size FROM chunks WHERE session_id = ?",
                (session["id"],),
            ).fetchall()
            for row in rows:
                path = chunk_path(session["id"], row["chunk_index"])
                if not path.is_file() or path.stat().st_size != row["size"]:
                    conn.execute(
                        "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                        (session["id"], row["chunk_index"]),
                    )


def expire_idle_sessions(now: datetime | None = None) -> list[str]:
    """Expire uploading sessions whose idle lease has elapsed.

    A session is marked `expired` and has its chunk rows/dir removed only if it
    is still `uploading` *and* still idle at the moment of the UPDATE — a
    chunk upload or status query racing the sweep refreshes `last_activity`
    and makes the guarded UPDATE match zero rows, so active sessions are
    never touched. Completed sessions are skipped outright, as are sessions
    currently being merged (the merge lock is taken non-blocking). Returns the
    list of expired session ids.
    """
    moment = now or datetime.now(timezone.utc)
    expired: list[str] = []
    with db.connect() as conn:
        candidates = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        stale = [s for s in candidates if db.lease_expired(s, moment)]

    for session in stale:
        sid = session["id"]
        if not merge_lock(sid).acquire(blocking=False):
            continue  # a merge is in progress; it beats the reaper
        try:
            moment_ts = moment.isoformat(timespec="milliseconds")
            with db.connect() as conn:
                cur = conn.execute(
                    "UPDATE sessions SET status = 'expired', expired_at = ? "
                    "WHERE id = ? AND status = 'uploading' AND last_activity = ?",
                    (moment_ts, sid, session["last_activity"]),
                )
                if cur.rowcount != 1:
                    continue  # refreshed or completed since the SELECT
                conn.execute("DELETE FROM chunks WHERE session_id = ?", (sid,))
            shutil.rmtree(session_dir(sid), ignore_errors=True)
            expired.append(sid)
            log.info("session %s expired idle lease at %s", sid, moment_ts)
        finally:
            merge_lock(sid).release()
    return expired


class MergeError(Exception):
    """Raised when chunks are not mergeable; carries the refreshed missing list."""

    def __init__(self, message: str, missing: list[int]):
        super().__init__(message)
        self.missing = missing


def merge_session(session_id: str) -> dict:
    """Verify and atomically merge all chunks into the final file.

    Every chunk's SHA-256 is re-verified against the DB record while streaming
    to the target; the merged file appears via os.replace() (atomic on POSIX),
    so observers only ever see a complete file or nothing.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise KeyError(session_id)
        if session["status"] == "completed":
            return db.session_payload(conn, session)
        if session["status"] == "expired":
            raise SessionExpired(session["expired_at"])
        missing = db.missing_indexes(conn, session)
        if missing:
            raise MergeError(f"{len(missing)} chunk(s) still missing", missing)
        rows = conn.execute(
            "SELECT chunk_index, size, sha256 FROM chunks "
            "WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

    final_path = final_path_for(session)
    tmp_path = final_path.with_name(final_path.name + ".tmp")
    overall = hashlib.sha256()
    corrupt: list[int] = []
    try:
        with open(tmp_path, "wb") as out:
            for row in rows:
                path = chunk_path(session_id, row["chunk_index"])
                if not path.is_file():
                    corrupt.append(row["chunk_index"])
                    continue
                hasher = hashlib.sha256()
                with open(path, "rb") as part:
                    while True:
                        buf = part.read(_COPY_BUFFER)
                        if not buf:
                            break
                        hasher.update(buf)
                        overall.update(buf)
                        out.write(buf)
                if hasher.hexdigest() != row["sha256"]:
                    corrupt.append(row["chunk_index"])
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    if corrupt:
        # Payload diverged from its recorded digest (e.g. disk corruption):
        # forget those chunks so the client can re-upload them.
        tmp_path.unlink(missing_ok=True)
        with db.connect() as conn:
            for index in corrupt:
                conn.execute(
                    "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                    (session_id, index),
                )
                chunk_path(session_id, index).unlink(missing_ok=True)
            session = db.get_session(conn, session_id)
            missing = db.missing_indexes(conn, session)
        raise MergeError(
            f"{len(corrupt)} chunk(s) failed re-verification and were discarded",
            missing,
        )

    digest = overall.hexdigest()
    declared = session["file_sha256"]
    if declared and declared.lower() != digest:
        tmp_path.unlink(missing_ok=True)
        raise MergeError(
            f"merged file sha256 {digest} does not match declared {declared}",
            missing=[],
        )

    os.replace(tmp_path, final_path)  # atomic publish

    with db.connect() as conn:
        # Status guard: the reaper cannot reach a session holding the merge
        # lock, but never overwrite an expired/completed row regardless.
        cur = conn.execute(
            "UPDATE sessions SET status = 'completed', final_path = ?, "
            "file_sha256 = ?, updated_at = ? WHERE id = ? AND status = 'uploading'",
            (str(final_path), digest, db.utcnow(), session_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            final_path.unlink(missing_ok=True)
            shutil.rmtree(session_dir(session_id), ignore_errors=True)
            refreshed = db.get_session(conn, session_id)
            if refreshed is not None and refreshed["status"] == "expired":
                raise SessionExpired(refreshed["expired_at"])
            raise MergeError("session is no longer uploading", missing=[])
        conn.execute("DELETE FROM chunks WHERE session_id = ?", (session_id,))
        session = db.get_session(conn, session_id)
    shutil.rmtree(session_dir(session_id), ignore_errors=True)
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        return db.session_payload(conn, session)
