"""End-to-end tests for the chunked upload service.

Run with:  DATA_DIR=$(mktemp -d) pytest tests/
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

# Point storage at a throwaway directory BEFORE importing the app.
_TMP = tempfile.mkdtemp(prefix="chunked-upload-test-")
os.environ["DATA_DIR"] = _TMP

from fastapi.testclient import TestClient  # noqa: E402

from app import config, db, storage  # noqa: E402
from app.main import app  # noqa: E402


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_file(chunk_size: int, total_chunks: int, last_size: int) -> tuple[bytes, list[bytes]]:
    chunks = []
    for i in range(total_chunks):
        size = last_size if i == total_chunks - 1 else chunk_size
        chunks.append(bytes(f"chunk-{i:05d}-".encode()) + os.urandom(size - 12))
    return b"".join(chunks), chunks


def _create(client: TestClient, data: bytes, chunk_size: int, **extra) -> dict:
    resp = client.post(
        "/uploads",
        json={
            "filename": "sample.bin",
            "total_size": len(data),
            "chunk_size": chunk_size,
            **extra,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _put(client: TestClient, sid: str, index: int, payload: bytes, digest: str | None = None):
    return client.put(
        f"/uploads/{sid}/chunks/{index}",
        content=payload,
        headers={"X-Chunk-SHA256": digest or _sha256(payload)},
    )


def test_full_flow_out_of_order_with_retry():
    with TestClient(app) as client:
        chunk_size = 1024
        data, chunks = _make_file(chunk_size, total_chunks=5, last_size=500)
        session = _create(client, data, chunk_size, file_sha256=_sha256(data))
        sid = session["session_id"]
        assert session["total_chunks"] == 5
        assert session["missing_chunks"] == [0, 1, 2, 3, 4]

        # Corrupted payload must be rejected and not persisted.
        bad = _put(client, sid, 0, chunks[0], digest="0" * 64)
        assert bad.status_code == 400
        assert client.get(f"/uploads/{sid}").json()["missing_chunks"] == [0, 1, 2, 3, 4]

        # Out-of-order upload: 3, 1, 4 ...
        for i in (3, 1, 4):
            resp = _put(client, sid, i, chunks[i])
            assert resp.status_code == 200, resp.text
        state = client.get(f"/uploads/{sid}").json()
        assert state["missing_chunks"] == [0, 2]
        assert state["received_count"] == 3

        # Retry of an already-received chunk is idempotent.
        assert _put(client, sid, 3, chunks[3]).status_code == 200
        assert client.get(f"/uploads/{sid}").json()["received_count"] == 3

        # Cannot merge while chunks are missing.
        early = client.post(f"/uploads/{sid}/complete")
        assert early.status_code == 409
        assert early.json()["detail"]["missing_chunks"] == [0, 2]

        # Finish the rest and merge.
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert _put(client, sid, 2, chunks[2]).status_code == 200
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 200, done.text
        body = done.json()
        assert body["status"] == "completed"
        assert body["file_sha256"] == _sha256(data)

        final_path = body["final_path"]
        assert os.path.isfile(final_path)
        with open(final_path, "rb") as fh:
            assert fh.read() == data

        # Complete is idempotent; further chunk uploads are refused.
        assert client.post(f"/uploads/{sid}/complete").status_code == 200
        assert _put(client, sid, 0, chunks[0]).status_code == 409


def test_restart_resumes_unfinished_session():
    chunk_size = 512
    data, chunks = _make_file(chunk_size, total_chunks=3, last_size=512)

    with TestClient(app) as client:
        sid = _create(client, data, chunk_size)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert _put(client, sid, 2, chunks[2]).status_code == 200

    # "Restart": a brand-new app instance over the same DATA_DIR.
    with TestClient(app) as client:
        state = client.get(f"/uploads/{sid}").json()
        assert state["status"] == "uploading"
        assert state["missing_chunks"] == [1]

        assert _put(client, sid, 1, chunks[1]).status_code == 200
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 200, done.text
        with open(done.json()["final_path"], "rb") as fh:
            assert fh.read() == data


def test_validation_errors():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=100)
        sid = _create(client, data, 256)["session_id"]

        # Index out of range.
        assert _put(client, sid, 2, chunks[0]).status_code == 400
        # Missing digest.
        assert client.put(f"/uploads/{sid}/chunks/0", content=chunks[0]).status_code == 400
        # Wrong size.
        assert _put(client, sid, 0, chunks[0][:-1]).status_code == 400
        # Unknown session.
        assert client.get("/uploads/nope").status_code == 404
        # Bad overall digest must fail the merge with 422.
        wrong = _create(client, data, 256, file_sha256="f" * 64)
        wsid = wrong["session_id"]
        assert _put(client, wsid, 0, chunks[0]).status_code == 200
        assert _put(client, wsid, 1, chunks[1]).status_code == 200
        assert client.post(f"/uploads/{wsid}/complete").status_code == 422


def test_abort_session():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert client.delete(f"/uploads/{sid}").status_code == 204
        assert client.get(f"/uploads/{sid}").status_code == 404


# ---------------------------------------------------------------- downloads


def _completed(client: TestClient, data: bytes, chunk_size: int) -> dict:
    """Upload every chunk of `data`, merge, and return the completed payload."""
    session = _create(client, data, chunk_size, file_sha256=_sha256(data))
    sid = session["session_id"]
    for index, offset in enumerate(range(0, len(data), chunk_size)):
        assert _put(client, sid, index, data[offset : offset + chunk_size]).status_code == 200
    done = client.post(f"/uploads/{sid}/complete")
    assert done.status_code == 200, done.text
    return done.json()


def test_download_full_file_with_etag():
    with TestClient(app) as client:
        data = os.urandom(5000)
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        etag = f'"{_sha256(data)}"'

        resp = client.get(f"/uploads/{sid}/content")
        assert resp.status_code == 200
        assert resp.content == data
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert resp.headers["Content-Length"] == str(len(data))
        assert resp.headers["ETag"] == etag  # strong validator: "<sha256>"
        assert "Content-Range" not in resp.headers

        # If-None-Match hit -> 304 with no body; weak form matches too.
        for header in (etag, f"W/{etag}", f'"{"0" * 64}", {etag}', "*"):
            cached = client.get(
                f"/uploads/{sid}/content", headers={"If-None-Match": header}
            )
            assert cached.status_code == 304, header
            assert cached.headers["ETag"] == etag
            assert cached.content == b""

        # A stale validator gets the full file again.
        stale = client.get(
            f"/uploads/{sid}/content", headers={"If-None-Match": f'"{"0" * 64}"'}
        )
        assert stale.status_code == 200
        assert stale.content == data


def test_download_single_ranges():
    with TestClient(app) as client:
        data = bytes(range(256)) * 20  # 5120 deterministic bytes
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        size = len(data)
        url = f"/uploads/{sid}/content"

        # bytes=start-end
        resp = client.get(url, headers={"Range": "bytes=100-199"})
        assert resp.status_code == 206
        assert resp.content == data[100:200]
        assert resp.headers["Content-Range"] == f"bytes 100-199/{size}"
        assert resp.headers["Content-Length"] == "100"
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert resp.headers["ETag"] == f'"{_sha256(data)}"'

        # bytes=start-  (open-ended)
        resp = client.get(url, headers={"Range": f"bytes={size - 10}-"})
        assert resp.status_code == 206
        assert resp.content == data[-10:]
        assert resp.headers["Content-Range"] == f"bytes {size - 10}-{size - 1}/{size}"

        # bytes=-suffix
        resp = client.get(url, headers={"Range": "bytes=-25"})
        assert resp.status_code == 206
        assert resp.content == data[-25:]
        assert resp.headers["Content-Range"] == f"bytes {size - 25}-{size - 1}/{size}"

        # Suffix longer than the file selects the whole file (still 206).
        resp = client.get(url, headers={"Range": "bytes=-999999"})
        assert resp.status_code == 206
        assert resp.content == data
        assert resp.headers["Content-Range"] == f"bytes 0-{size - 1}/{size}"

        # End beyond EOF is clamped.
        resp = client.get(url, headers={"Range": f"bytes=0-{size + 100}"})
        assert resp.status_code == 206
        assert resp.content == data
        assert resp.headers["Content-Range"] == f"bytes 0-{size - 1}/{size}"


def test_download_range_errors_are_416():
    with TestClient(app) as client:
        data = os.urandom(1000)
        done = _completed(client, data, 256)
        sid = done["session_id"]
        url = f"/uploads/{sid}/content"

        bad_ranges = [
            "bytes=0-1,3-4",      # multiple ranges unsupported
            "bytes=abc",          # garbage
            "bytes=",             # empty spec
            "bytes=-",            # neither bound
            "items=0-1",          # unknown unit
            "bytes=5-2",          # start > end
            f"bytes={len(data)}-",   # starts at EOF
            f"bytes={len(data)}-{len(data) + 9}",  # fully out of bounds
            "bytes=-0",           # zero-length suffix
        ]
        for bad in bad_ranges:
            resp = client.get(url, headers={"Range": bad})
            assert resp.status_code == 416, bad
            assert resp.headers["Content-Range"] == f"bytes */{len(data)}"
            assert resp.headers["Accept-Ranges"] == "bytes"


def test_download_if_range():
    with TestClient(app) as client:
        data = os.urandom(2048)
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        url = f"/uploads/{sid}/content"
        etag = f'"{_sha256(data)}"'

        # Matching If-Range -> the range is honoured.
        resp = client.get(url, headers={"Range": "bytes=0-9", "If-Range": etag})
        assert resp.status_code == 206
        assert resp.content == data[:10]

        # Mismatched / weak / date-valued If-Range -> Range ignored, full 200.
        for if_range in (f'"{"0" * 64}"', f"W/{etag}", "Wed, 21 Oct 2015 07:28:00 GMT"):
            resp = client.get(url, headers={"Range": "bytes=0-9", "If-Range": if_range})
            assert resp.status_code == 200, if_range
            assert resp.content == data
            assert "Content-Range" not in resp.headers


def test_download_session_state_errors():
    with TestClient(app) as client:
        # Unknown session -> 404.
        assert client.get("/uploads/nope/content").status_code == 404

        # Unfinished session -> 409 (both before and during upload).
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256)["session_id"]
        assert client.get(f"/uploads/{sid}/content").status_code == 409
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert client.get(f"/uploads/{sid}/content").status_code == 409


# ------------------------------------------------------------- session leases


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _set_last_activity(sid: str, dt: datetime) -> None:
    with db.connect() as conn:
        conn.execute(
            "UPDATE sessions SET last_activity = ? WHERE id = ?", (_iso(dt), sid)
        )


def test_idle_lease_defaults_and_limits():
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=1, last_size=256)

        session = _create(client, data, 256)
        assert session["idle_timeout_seconds"] == 86400
        assert session["status"] == "uploading"
        assert session["expired_at"] is None
        # expires_at is the projected lease deadline = last_activity + timeout.
        last = datetime.fromisoformat(session["last_activity"])
        assert datetime.fromisoformat(session["expires_at"]) == last + timedelta(
            seconds=86400
        )

        # Explicit, in-range lease.
        short = _create(client, data, 256, idle_timeout_seconds=60)
        assert short["idle_timeout_seconds"] == 60

        # Out-of-range / invalid leases are rejected at creation.
        for bad in (0, -5, 604801, 10**9):
            resp = client.post(
                "/uploads",
                json={
                    "filename": "sample.bin",
                    "total_size": len(data),
                    "chunk_size": 256,
                    "idle_timeout_seconds": bad,
                },
            )
            assert resp.status_code == 422, bad


def test_reaper_marks_stale_session_expired_and_clears_chunks():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        chunk_file = storage.chunk_path(sid, 0)
        assert chunk_file.is_file()

        # Idle within the lease: nothing happens.
        _set_last_activity(sid, datetime.now(timezone.utc) - timedelta(seconds=30))
        assert storage.expire_idle_sessions() == []
        assert client.get(f"/uploads/{sid}").json()["status"] == "uploading"
        assert chunk_file.is_file()

        # Past the lease: expired, with expired_at recorded and chunks gone.
        expiry = datetime.now(timezone.utc) - timedelta(seconds=1)
        _set_last_activity(sid, expiry - timedelta(seconds=60))
        reaped = storage.expire_idle_sessions(now=expiry)
        assert reaped == [sid]
        assert not chunk_file.exists()
        with db.connect() as conn:
            assert db.received_indexes(conn, sid) == set()

        state = client.get(f"/uploads/{sid}").json()
        assert state["status"] == "expired"
        assert state["expired_at"] == _iso(expiry)
        assert state["missing_chunks"] == []
        assert state["received_count"] == 0
        assert state["expires_at"] is None

        # Upload and complete are 410 Gone carrying expired_at.
        gone = _put(client, sid, 1, chunks[1])
        assert gone.status_code == 410
        assert gone.json()["detail"]["expired_at"] == _iso(expiry)
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 410
        assert done.json()["detail"]["expired_at"] == _iso(expiry)

        # Reaping again is idempotent; expired rows are not touched.
        assert storage.expire_idle_sessions() == []

        # Expired sessions are omitted from the resumable list.
        ids = [s["session_id"] for s in client.get("/uploads").json()["sessions"]]
        assert sid not in ids


def test_chunk_upload_and_status_query_refresh_lease():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]

        stale = datetime.now(timezone.utc) - timedelta(seconds=59)
        _set_last_activity(sid, stale)

        # A status query counts as activity and pushes the deadline forward.
        resp = client.get(f"/uploads/{sid}")
        assert resp.status_code == 200
        last_after_query = datetime.fromisoformat(resp.json()["last_activity"])
        assert last_after_query > stale
        assert storage.expire_idle_sessions() == []

        # A successful chunk upload refreshes the lease as well.
        _set_last_activity(sid, stale)
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        state = client.get(f"/uploads/{sid}").json()
        assert datetime.fromisoformat(state["last_activity"]) > stale
        assert storage.expire_idle_sessions() == []
        assert state["status"] == "uploading"


def test_get_session_after_create_returns_200_and_renews_lease():
    """Regression: GET /uploads/{id} right after a successful create is 200.

    A freshly created session must be immediately queryable; the payload
    carries received_count / missing_chunks / last_activity / expires_at, and
    the query itself renews the lease. Unknown ids stay 404; expired sessions
    remain readable but are not renewed.
    """
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=2, last_size=256)
        session = _create(client, data, 256, idle_timeout_seconds=60)
        sid = session["session_id"]

        # The exact reported flow: create -> GET must answer 200, never 404.
        resp = client.get(f"/uploads/{sid}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "uploading"
        assert body["received_count"] == 0
        assert body["missing_chunks"] == [0, 1]
        # Lease fields are present and coherent: expires_at = last_activity + lease.
        last = datetime.fromisoformat(body["last_activity"])
        assert datetime.fromisoformat(body["expires_at"]) == last + timedelta(seconds=60)

        # The query renews the lease (backdate first so the refresh is visible).
        stale = datetime.now(timezone.utc) - timedelta(seconds=30)
        _set_last_activity(sid, stale)
        refreshed = client.get(f"/uploads/{sid}").json()
        assert datetime.fromisoformat(refreshed["last_activity"]) > stale

        # Unknown session ids still 404.
        assert client.get("/uploads/no-such-session").status_code == 404

        # Expired sessions remain readable (200) but are not renewed.
        expiry = datetime.now(timezone.utc) - timedelta(seconds=1)
        _set_last_activity(sid, expiry - timedelta(seconds=60))
        assert storage.expire_idle_sessions(now=expiry) == [sid]
        first = client.get(f"/uploads/{sid}")
        assert first.status_code == 200
        assert first.json()["status"] == "expired"
        second = client.get(f"/uploads/{sid}").json()
        assert second["last_activity"] == first.json()["last_activity"]
        assert second["expires_at"] is None
        assert second["expired_at"] == first.json()["expired_at"]


def test_completed_sessions_are_never_reaped():
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=2, last_size=256)
        done = _completed(client, data, 256)
        sid = done["session_id"]
        # Completed ages ago would be "idle" by any measure; it must survive.
        old = datetime.now(timezone.utc) - timedelta(days=30)
        with db.connect() as conn:
            conn.execute(
                "UPDATE sessions SET last_activity = ?, updated_at = ? WHERE id = ?",
                (_iso(old), _iso(old), sid),
            )
        assert storage.expire_idle_sessions() == []
        body = client.get(f"/uploads/{sid}").json()
        assert body["status"] == "completed"
        assert os.path.isfile(done["final_path"])
        assert client.get(f"/uploads/{sid}/content").status_code == 200


def test_reaper_skips_session_currently_merging():
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=1)["session_id"]
        _set_last_activity(sid, datetime.now(timezone.utc) - timedelta(hours=1))

        lock = storage.merge_lock(sid)
        assert lock.acquire(blocking=False)
        try:
            assert storage.expire_idle_sessions() == []
            with db.connect() as conn:
                assert db.get_session(conn, sid)["status"] == "uploading"
        finally:
            lock.release()

        # Once the merge finishes (lock released), the reaper may reap it.
        assert storage.expire_idle_sessions() == [sid]


def test_deleted_sessions_are_not_reaped():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=1)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert client.delete(f"/uploads/{sid}").status_code == 204
        _set_last_activity(sid, datetime.now(timezone.utc) - timedelta(hours=1))
        # Row is gone; the sweep simply has nothing to expire.
        assert storage.expire_idle_sessions() == []
        assert client.get(f"/uploads/{sid}").status_code == 404


def test_startup_recovery_sweep_expires_stale_sessions():
    data, chunks = _make_file(256, total_chunks=2, last_size=256)
    stale_sid = fresh_sid = None

    # First boot: create one stale and one still-leased session.
    with TestClient(app) as client:
        stale_sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        assert _put(client, stale_sid, 0, chunks[0]).status_code == 200
        _set_last_activity(
            stale_sid, datetime.now(timezone.utc) - timedelta(seconds=61)
        )
        fresh_sid = _create(client, data, 256, idle_timeout_seconds=3600)["session_id"]
        assert _put(client, fresh_sid, 0, chunks[0]).status_code == 200

    # Second boot: the lifespan recovery sweep must expire the stale one and
    # leave the fresh session resumable.
    with TestClient(app) as client:
        stale = client.get(f"/uploads/{stale_sid}").json()
        assert stale["status"] == "expired"
        assert stale["expired_at"] is not None
        assert not storage.chunk_path(stale_sid, 0).exists()
        assert client.post(f"/uploads/{stale_sid}/complete").status_code == 410

        fresh = client.get(f"/uploads/{fresh_sid}").json()
        assert fresh["status"] == "uploading"
        assert fresh["missing_chunks"] == [1]
        assert _put(client, fresh_sid, 1, chunks[1]).status_code == 200
        done = client.post(f"/uploads/{fresh_sid}/complete")
        assert done.status_code == 200, done.text


def test_init_db_migrates_pre_lease_database():
    # Hand-build a database from the pre-lease schema (no lease columns and
    # the old status CHECK), then let init_db() migrate it in place.
    migrated_dir = tempfile.mkdtemp(prefix="chunked-upload-migrate-")
    db_path = os.path.join(migrated_dir, "uploads.db")
    created = old_updated = _iso(datetime.now(timezone.utc) - timedelta(days=2))
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                total_size INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                total_chunks INTEGER NOT NULL,
                file_sha256 TEXT,
                status TEXT NOT NULL DEFAULT 'uploading'
                    CHECK (status IN ('uploading', 'completed')),
                final_path TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE chunks (
                session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                received_at TEXT NOT NULL,
                PRIMARY KEY (session_id, chunk_index)
            );
            """
        )
        conn.execute(
            "INSERT INTO sessions (id, filename, total_size, chunk_size, "
            "total_chunks, status, created_at, updated_at) "
            "VALUES ('old1', 'a.bin', 256, 256, 1, 'uploading', ?, ?)",
            (created, old_updated),
        )
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at) "
            "VALUES ('old1', 0, 256, ?, ?)",
            ("a" * 64, old_updated),
        )
        conn.commit()
    finally:
        conn.close()

    original = config.DB_PATH
    config.DB_PATH = type(original)(db_path)
    try:
        db.init_db()
        with db.connect() as migrated:
            row = db.get_session(migrated, "old1")
            assert row is not None
            assert row["idle_timeout_seconds"] == config.DEFAULT_IDLE_TIMEOUT_SECONDS
            assert row["last_activity"] == old_updated
            assert row["expired_at"] is None
            # Chunk rows and the FK survive the rebuild.
            assert db.received_indexes(migrated, "old1") == {0}
        # Migration is idempotent: a second init is a no-op.
        db.init_db()
    finally:
        config.DB_PATH = original

