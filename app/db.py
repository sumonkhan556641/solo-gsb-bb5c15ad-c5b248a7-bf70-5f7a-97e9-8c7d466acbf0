"""SQLite persistence layer.

The database is the source of truth for session/chunk metadata; chunk payloads
live as files on disk next to it. Both reside under DATA_DIR, so a restart (or a
new container with the same volume) picks up unfinished sessions transparently.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                    TEXT PRIMARY KEY,
    filename              TEXT NOT NULL,
    total_size            INTEGER NOT NULL,
    chunk_size            INTEGER NOT NULL,
    total_chunks          INTEGER NOT NULL,
    file_sha256           TEXT,   -- optional overall digest, checked at merge
    status                TEXT NOT NULL DEFAULT 'uploading'
                          CHECK (status IN ('uploading', 'completed', 'expired')),
    final_path            TEXT,
    idle_timeout_seconds  INTEGER NOT NULL DEFAULT 86400,
    last_activity         TEXT NOT NULL,
    expired_at            TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

-- Only live, reaper-scannable rows carry a lease deadline.
CREATE INDEX IF NOT EXISTS idx_sessions_lease
    ON sessions (last_activity, idle_timeout_seconds) WHERE status = 'uploading';

CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""

# Columns added when the session-lease feature was introduced; databases
# created by earlier versions are migrated to the current schema in place.
_LEASE_COLUMNS = ("idle_timeout_seconds", "last_activity", "expired_at")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_ts(value: str) -> datetime:
    """Parse a value emitted by utcnow(); always timezone-aware UTC."""
    return datetime.fromisoformat(value)


def connect() -> sqlite3.Connection:
    """Open a short-lived connection. SQLite handles concurrency via WAL."""
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create the schema, migrating pre-lease databases in place.

    SQLite cannot add a CHECK constraint to an existing table, so an old
    `sessions` table is rebuilt via the rename/copy/drop dance on a dedicated
    connection with foreign keys disabled.
    """
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    needs_migration = False
    with connect() as conn:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sessions'"
        ).fetchone():
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
            }
            needs_migration = any(col not in columns for col in _LEASE_COLUMNS)
        if not needs_migration:
            conn.executescript(SCHEMA)
    if needs_migration:
        _migrate_pre_lease_schema()


def _migrate_pre_lease_schema() -> None:
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        # Must be toggled before any write starts a transaction; legacy_alter
        # keeps the chunks-table FK pointed at the renamed (old) table.
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA legacy_alter_table=ON")
        conn.execute("ALTER TABLE sessions RENAME TO sessions_legacy")
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO sessions (id, filename, total_size, chunk_size, "
            "total_chunks, file_sha256, status, final_path, "
            "idle_timeout_seconds, last_activity, expired_at, "
            "created_at, updated_at) "
            "SELECT id, filename, total_size, chunk_size, total_chunks, "
            "file_sha256, status, final_path, ?, updated_at, NULL, "
            "created_at, updated_at FROM sessions_legacy",
            (config.DEFAULT_IDLE_TIMEOUT_SECONDS,),
        )
        conn.execute("DROP TABLE sessions_legacy")
        conn.commit()
    finally:
        conn.close()


def get_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()


def received_indexes(conn: sqlite3.Connection, session_id: str) -> set[int]:
    rows = conn.execute(
        "SELECT chunk_index FROM chunks WHERE session_id = ?", (session_id,)
    ).fetchall()
    return {row["chunk_index"] for row in rows}


def missing_indexes(conn: sqlite3.Connection, session: sqlite3.Row) -> list[int]:
    have = received_indexes(conn, session["id"])
    return [i for i in range(session["total_chunks"]) if i not in have]


def lease_expired(session: sqlite3.Row, now: datetime | None = None) -> bool:
    """True iff an uploading session's idle lease has elapsed."""
    if session["status"] != "uploading":
        return False
    moment = now or datetime.now(timezone.utc)
    last = parse_ts(session["last_activity"])
    elapsed = (moment - last).total_seconds()
    return elapsed >= session["idle_timeout_seconds"]


def expiry_timestamp(session: sqlite3.Row, now: datetime | None = None) -> str | None:
    """When the lease runs out, or None for sessions without an active lease."""
    if session["status"] != "uploading":
        return None
    moment = now or datetime.now(timezone.utc)
    last = parse_ts(session["last_activity"])
    deadline_ts = last.timestamp() + session["idle_timeout_seconds"]
    deadline = datetime.fromtimestamp(deadline_ts, tz=timezone.utc)
    return deadline.isoformat(timespec="milliseconds")


def touch_session(conn: sqlite3.Connection, session_id: str, moment: str) -> None:
    """Refresh the lease: successful chunk uploads and status queries."""
    conn.execute(
        "UPDATE sessions SET last_activity = ? WHERE id = ? AND status = 'uploading'",
        (moment, session_id),
    )


def session_payload(
    conn: sqlite3.Connection,
    session: sqlite3.Row,
    now: datetime | None = None,
) -> dict:
    """Public representation of a session, including missing chunk indexes."""
    missing = missing_indexes(conn, session)
    total = session["total_chunks"]
    payload = {
        "session_id": session["id"],
        "filename": session["filename"],
        "status": session["status"],
        "total_size": session["total_size"],
        "chunk_size": session["chunk_size"],
        "total_chunks": total,
        "received_count": total - len(missing),
        "missing_chunks": missing,
        "file_sha256": session["file_sha256"],
        "final_path": session["final_path"],
        "idle_timeout_seconds": session["idle_timeout_seconds"],
        "last_activity": session["last_activity"],
        "expires_at": expiry_timestamp(session, now),
        "expired_at": session["expired_at"],
        "created_at": session["created_at"],
        "updated_at": session["updated_at"],
    }
    # Expired sessions have no chunks, so no indexes are "missing" to retry.
    if session["status"] == "expired":
        payload["missing_chunks"] = []
        payload["received_count"] = 0
    return payload
