"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

# Root directory for all persistent state (SQLite DB, chunk files, merged files).
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()

# Subdirectory layout inside DATA_DIR.
SESSIONS_DIRNAME = "sessions"  # one subdir per upload session, holds .part chunks
FILES_DIRNAME = "files"        # merged, completed files
DB_FILENAME = "uploads.db"

# Port the container/uvicorn listens on (documentational; uvicorn flag wins).
PORT = int(os.environ.get("PORT", "8000"))

# Upper bound for a single chunk (64 MiB) to avoid pathological allocations.
MAX_CHUNK_SIZE = 64 * 1024 * 1024

# Upload-session lease defaults. A session with no successful chunk upload or
# status query for IDLE_TIMEOUT seconds is expired by the background reaper
# (and by the startup recovery sweep); clients may request a shorter lease at
# creation, up to MAX_IDLE_TIMEOUT.
DEFAULT_IDLE_TIMEOUT_SECONDS = 24 * 60 * 60       # 1 day
MAX_IDLE_TIMEOUT_SECONDS = 7 * 24 * 60 * 60       # 7 days

# How often the background reaper scans for idle sessions (seconds).
REAPER_INTERVAL_SECONDS = max(
    1.0, float(os.environ.get("REAPER_INTERVAL_SECONDS", "300"))
)

DB_PATH = DATA_DIR / DB_FILENAME


def sessions_dir() -> Path:
    return DATA_DIR / SESSIONS_DIRNAME


def files_dir() -> Path:
    return DATA_DIR / FILES_DIRNAME


def ensure_dirs() -> None:
    """Create the storage layout. Called at startup; safe to call repeatedly."""
    sessions_dir().mkdir(parents=True, exist_ok=True)
    files_dir().mkdir(parents=True, exist_ok=True)
