"""
Observable fetch-attempt recording for OpenF1 calls.

Used by cron/fetch_race_results.py and cron/race_manager.py.  No alerting here —
that belongs to an external observer (BUD-164).  This module only persists what
happened so the observer and operators can query it.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class Outcome(str, Enum):
    OK = "ok"
    EMPTY = "empty"               # race not finished yet / incomplete podium
    HTTP_ERROR = "http_error"     # 4xx/5xx after retries
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"


EXPECTED_RACE_DURATION_SEC = 4 * 60 * 60  # 4 hours


class FetchFailure(Exception):
    """
    A fetch outcome that should fail the cron Job: exhausted retries, or an
    empty result for a race that finished long ago. Callers let this
    propagate out of main() so the process exits non-zero and the
    Kubernetes Job is recorded as Failed. A transient failure that succeeds
    on retry never raises this — retries happen inside openf1._get() before
    it either returns data or raises OpenF1Error.
    """

    def __init__(self, message: str, outcome: "Outcome"):
        super().__init__(message)
        self.outcome = outcome


def outcome_for_error(exc: Exception) -> Outcome:
    """
    Classify an OpenF1Error (or any exception exposing status_code /
    is_timeout, as openf1.OpenF1Error does) into a fetch_attempts outcome.
    """
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return Outcome.RATE_LIMITED
    if getattr(exc, "is_timeout", False):
        return Outcome.TIMEOUT
    return Outcome.HTTP_ERROR


def ensure_fetch_attempts_table(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_attempts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            attempted_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            endpoint      TEXT NOT NULL,
            cache_key     TEXT NOT NULL,
            outcome       TEXT NOT NULL
                          CHECK (outcome IN ('ok','empty','http_error','timeout','rate_limited')),
            http_status   INTEGER,
            session_key   INTEGER,
            race_id       INTEGER,
            detail        TEXT
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fetch_attempts_race_attempted
            ON fetch_attempts(race_id, attempted_at DESC)
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fetch_attempts_endpoint_attempted
            ON fetch_attempts(endpoint, attempted_at DESC)
        """
    )
    db.commit()


def record_fetch_attempt(
    db: sqlite3.Connection,
    endpoint: str,
    cache_key: str,
    outcome: Outcome,
    *,
    http_status: Optional[int] = None,
    session_key: Optional[int] = None,
    race_id: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    ensure_fetch_attempts_table(db)
    db.execute(
        """
        INSERT INTO fetch_attempts
            (attempted_at, endpoint, cache_key, outcome, http_status, session_key, race_id, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            endpoint,
            cache_key,
            outcome.value,
            http_status,
            session_key,
            race_id,
            detail,
        ),
    )
    db.commit()


def last_successful_fetch_at(db: sqlite3.Connection) -> Optional[datetime]:
    """Timestamp of the most recent 'ok' fetch_attempt, or None."""
    ensure_fetch_attempts_table(db)
    row = db.execute(
        """
        SELECT attempted_at FROM fetch_attempts
        WHERE outcome = ?
        ORDER BY attempted_at DESC
        LIMIT 1
        """,
        (Outcome.OK.value,),
    ).fetchone()
    if not row:
        return None
    stamp = row["attempted_at"] if isinstance(row, sqlite3.Row) else row[0]
    try:
        dt = datetime.fromisoformat(stamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def has_recent_ok_fetch(db: sqlite3.Connection, max_age_seconds: int) -> bool:
    """True if there is an 'ok' attempt within max_age_seconds."""
    last = last_successful_fetch_at(db)
    if last is None:
        return False
    return (datetime.now(timezone.utc) - last).total_seconds() <= max_age_seconds


def _parse_race_dt(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    s = str(date_str).strip().replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def race_finished_long_ago(race_date: str, now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(timezone.utc)
    start = _parse_race_dt(race_date)
    if start is None:
        return False
    return (now - start).total_seconds() > EXPECTED_RACE_DURATION_SEC
