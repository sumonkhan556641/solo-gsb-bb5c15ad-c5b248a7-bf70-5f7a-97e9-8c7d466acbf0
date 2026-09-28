"""On-disk layout and merge logic for chunk payloads."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import sqlite3
import threading
import uuid
import zipfile
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from . import config, db

_COPY_BUFFER = 1024 * 1024  # 1 MiB streaming buffer for merge/verify/archive

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


# ---------------------------------------------------------------- batch archives


class ArchiveError(Exception):
    """A session cannot be included in an archive export.

    Carries the HTTP status the endpoint should answer with and the id of the
    offending session so clients can tell which entry blocked the request.
    """

    def __init__(self, message: str, *, status_code: int, session_id: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.session_id = session_id


def archive_path(archive_id: str) -> Path:
    return config.archives_dir() / f"{archive_id}.zip"


def unique_entry_name(filename: str, used: set[str]) -> str:
    """Return a stable, collision-free ZIP entry name, preserving extension.

    The first use of a name wins; later duplicates get a numeric stem suffix:
    `report.pdf`, `report_1.pdf`, `report_2.pdf`. Names without an extension
    take the suffix at the end (`README`, `README_1`). Selection is fully
    deterministic, so the same request order always yields the same entries.
    """
    candidate = filename
    counter = 1
    while candidate in used:
        stem, ext = os.path.splitext(filename)
        candidate = f"{stem}_{counter}{ext}"
        counter += 1
    used.add(candidate)
    return candidate


def build_archive(session_ids: list[str]) -> dict:
    """Bundle completed sessions' files into one ZIP, honoring the given order.

    Every source session must be `completed` with its merged file still on
    disk. All source files are opened and *pinned* before the archive is
    written — an open file descriptor keeps the bytes readable even if the
    session is deleted concurrently, so a source can never vanish halfway and
    leave a partial export. The ZIP is streamed through bounded buffers into a
    temp file, atomically renamed into place, and only then recorded in the
    database; any failure unwinds both with nothing downloadable left behind.
    """
    # Phase 1: validate every session and resolve its source file. No side
    # effects yet, so a failure at this point leaves nothing behind at all.
    resolved: list[tuple[int, str, str, Path]] = []
    with db.connect() as conn:
        for position, sid in enumerate(session_ids):
            session = db.get_session(conn, sid)
            if session is None:
                raise ArchiveError(
                    "session not found", status_code=404, session_id=sid
                )
            if session["status"] != "completed" or not session["final_path"]:
                raise ArchiveError(
                    "session is not completed; only completed sessions can be archived",
                    status_code=409,
                    session_id=sid,
                )
            path = Path(session["final_path"])
            if not path.is_file():
                raise ArchiveError(
                    "completed file is missing on disk",
                    status_code=404,
                    session_id=sid,
                )
            resolved.append((position, sid, session["filename"], path))

    archive_id = uuid.uuid4().hex
    final_path = archive_path(archive_id)
    tmp_path = final_path.with_name(f".{final_path.name}.tmp-{uuid.uuid4().hex}")

    # Phase 2: pin *all* sources open before writing anything.
    pinned: list[BinaryIO] = []
    try:
        sources: list[tuple[int, str, str, str, int, BinaryIO]] = []
        used_names: set[str] = set()
        for position, sid, original_name, path in resolved:
            try:
                fh = open(path, "rb")
            except FileNotFoundError:
                raise ArchiveError(
                    "completed file is missing on disk",
                    status_code=404,
                    session_id=sid,
                )
            pinned.append(fh)
            size = os.fstat(fh.fileno()).st_size
            entry_name = unique_entry_name(sanitize_filename(original_name), used_names)
            sources.append((position, sid, original_name, entry_name, size, fh))

        # Phase 3: stream the ZIP to a temp file with bounded buffers.
        try:
            with zipfile.ZipFile(
                tmp_path, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as zf:
                for position, sid, _original, entry_name, _size, fh in sources:
                    info = zipfile.ZipInfo(
                        filename=entry_name, date_time=(1980, 1, 1, 0, 0, 0)
                    )
                    info.compress_type = zipfile.ZIP_DEFLATED
                    fh.seek(0)
                    with zf.open(info, "w") as out:
                        shutil.copyfileobj(fh, out, length=_COPY_BUFFER)
            archive_size = tmp_path.stat().st_size

            # Phase 4: atomic publish, then record. If the record cannot be
            # written the published file is removed, so no archive is ever
            # downloadable without a matching record (and vice versa).
            os.replace(tmp_path, final_path)
            try:
                now = db.utcnow()
                with db.connect() as conn:
                    conn.execute(
                        "INSERT INTO archives (id, size, created_at) VALUES (?, ?, ?)",
                        (archive_id, archive_size, now),
                    )
                    for position, sid, original_name, entry_name, size, _fh in sources:
                        conn.execute(
                            "INSERT INTO archive_entries (archive_id, position, "
                            "session_id, source_filename, entry_name, size) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (archive_id, position, sid, original_name, entry_name, size),
                        )
                    archive = db.get_archive(conn, archive_id)
                    return db.archive_payload(conn, archive)
            except BaseException:
                final_path.unlink(missing_ok=True)
                raise
        except ArchiveError:
            raise
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
    finally:
        for fh in pinned:
            fh.close()


def delete_archive(archive_id: str) -> bool:
    """Delete an archive record (entries cascade) and its ZIP.

    Returns False when no such archive exists. The ZIP unlink is best-effort
    after the record is gone, mirroring session deletion.
    """
    with db.connect() as conn:
        archive = db.get_archive(conn, archive_id)
        if archive is None:
            return False
        conn.execute("DELETE FROM archives WHERE id = ?", (archive_id,))
    archive_path(archive_id).unlink(missing_ok=True)
    return True


def reconcile_archives() -> None:
    """Drop leftover files from an archive operation interrupted by a crash.

    Removes temp archives (hidden `.…tmp-…` files) and published ZIPs with no
    database record; neither is ever reachable through the API, and clearing
    them on boot keeps storage from accumulating debris.
    """
    root = config.archives_dir()
    if not root.is_dir():
        return
    with db.connect() as conn:
        live = {row["id"] for row in conn.execute("SELECT id FROM archives")}
    for path in root.iterdir():
        if not path.is_file():
            continue
        if path.name.startswith(".") and ".tmp-" in path.name:
            path.unlink(missing_ok=True)
        elif path.suffix == ".zip" and path.stem not in live:
            path.unlink(missing_ok=True)
