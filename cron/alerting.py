#!/usr/bin/env python3
"""
Alerting logic for the F1 results fetch pipeline.

Provides:
  * record_fetch_attempt(db, race_id, stage, outcome, error=None)
  * alert_if_needed(db)

The goal is simple: when the cron job cannot obtain results for a completed
race, Brett gets one Telegram alert per failure cycle.  A "failure cycle" is
a transition from "everything fine" to "a fetch has failed".  Once alerted,
subsequent cron runs are silent until the problem is resolved, so there is no
spam.

Tables created here are additive only; they do not replace existing app schema.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from telegram_alert import send_telegram_alert, TelegramAlertError, alert_enabled

logger = logging.getLogger(__name__)

# How long after the race start we consider the race "completed" even if no
# official results exist yet.  This is the window during which an empty result
# is treated as an actionable failure rather than "race not finished".
EXPECTED_RACE_DURATION = 4 * 60 * 60  # 4 hours


class FetchOutcome:
    OK = "ok"
    NO_RESULTS_YET = "no_results_yet"
    API_ERROR = "api_error"
    INCOMPLETE_PODIUM = "incomplete_podium"
    UNRESOLVED_DRIVER = "unresolved_driver"


def ensure_alert_tables(db: sqlite3.Connection) -> None:
    """Create the small schema used by this module."""
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS fetch_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER NOT NULL,
            stage TEXT NOT NULL,
            outcome TEXT NOT NULL,
            error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_fetch_attempts_race_created
            ON fetch_attempts(race_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS fetch_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER NOT NULL,
            alert_sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_fetch_alerts_race_sent
            ON fetch_alerts(race_id, alert_sent_at DESC);
        """
    )
    db.commit()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def record_fetch_attempt(
    db: sqlite3.Connection,
    race_id: int,
    stage: str,
    outcome: str,
    error: Optional[str] = None,
) -> None:
    """Log a single fetch outcome.  A None race_id is stored as a sentinel
    for attempts where the race cannot yet be determined (e.g. session-key
    lookup failure in _fetch_podium).  Alerting is skipped for sentinel rows.
    """
    ensure_alert_tables(db)
    db.execute(
        """
        INSERT INTO fetch_attempts (race_id, stage, outcome, error, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (race_id if race_id is not None else -1, stage, outcome, error, _utcnow().isoformat()),
    )
    db.commit()


def _is_actionable_failure(outcome: str) -> bool:
    """Failures that should alert once per incident, not transient not-ready."""
    return outcome in {
        FetchOutcome.API_ERROR,
        FetchOutcome.INCOMPLETE_PODIUM,
        FetchOutcome.UNRESOLVED_DRIVER,
    }


def _race_started_long_ago(race_date: str, now: Optional[datetime] = None) -> bool:
    """True if the race started more than EXPECTED_RACE_DURATION ago."""
    now = now or _utcnow()
    try:
        start = datetime.fromisoformat(race_date.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return False
    return (now - start).total_seconds() > EXPECTED_RACE_DURATION


def _active_alert_for_race(db: sqlite3.Connection, race_id: int) -> Optional[int]:
    """Return the id of an unresolved alert for this race, if any."""
    row = db.execute(
        """
        SELECT id FROM fetch_alerts
        WHERE race_id = ? AND resolved_at IS NULL
        ORDER BY alert_sent_at DESC
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    return row[0] if row else None


def _last_attempt(db: sqlite3.Connection, race_id: int) -> Optional[dict]:
    """Most recent fetch attempt for a race."""
    row = db.execute(
        """
        SELECT stage, outcome, error, created_at FROM fetch_attempts
        WHERE race_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row) if isinstance(row, sqlite3.Row) else {
        "stage": row[0], "outcome": row[1], "error": row[2], "created_at": row[3]
    }


def _last_real_attempt(db: sqlite3.Connection, race_id: int) -> Optional[dict]:
    """Most recent fetch attempt for a race (skipping -1 sentinel rows)."""
    row = db.execute(
        """
        SELECT stage, outcome, error, created_at FROM fetch_attempts
        WHERE race_id = ? AND race_id != -1
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (race_id,),
    ).fetchone()
    if not row:
        return None
    return dict(row) if isinstance(row, sqlite3.Row) else {
        "stage": row[0], "outcome": row[1], "error": row[2], "created_at": row[3]
    }


def _resolve_alert(db: sqlite3.Connection, alert_id: int) -> None:
    """Mark an alert resolved."""
    db.execute(
        "UPDATE fetch_alerts SET resolved_at = ? WHERE id = ?",
        (_utcnow().isoformat(), alert_id),
    )
    db.commit()


def _send_alert(db: sqlite3.Connection, race_id: int, attempt: dict) -> None:
    """Send one Telegram alert and record it."""
    stage = attempt.get("stage", "unknown")
    outcome = attempt.get("outcome", "unknown")
    error = attempt.get("error") or ""
    created_at = attempt.get("created_at") or _utcnow().isoformat()

    lines = [
        "🚨 <b>F1 results fetch failure</b>",
        "",
        f"Race ID: <code>{race_id}</code>",
        f"Stage: {stage}",
        f"Outcome: {outcome}",
        f"Detected: {created_at}",
    ]
    if error:
        lines.append(f"Error: {error[:500]}")

    message = "\n".join(lines)

    try:
        send_telegram_alert(message)
    except TelegramAlertError as exc:
        logger.error("Failed to send Telegram alert: %s", exc)
        return

    db.execute(
        "INSERT INTO fetch_alerts (race_id, alert_sent_at) VALUES (?, ?)",
        (race_id, _utcnow().isoformat()),
    )
    db.commit()
    logger.info("Alerted for race %s failure (outcome=%s)", race_id, outcome)


def alert_if_needed(
    db: sqlite3.Connection,
    race_id: int,
    race_date: str,
    now: Optional[datetime] = None,
) -> None:
    """
    Send a Telegram alert if the latest fetch attempt is an actionable failure
    for a race that should already be complete, and no unresolved alert exists.
    Resolves existing alerts when the latest attempt is OK.
    """
    ensure_alert_tables(db)
    now = now or _utcnow()

    if not alert_enabled():
        logger.debug("Telegram alerting disabled (missing env vars)")
        return

    active_alert_id = _active_alert_for_race(db, race_id)

    last_ok = db.execute(
        """
        SELECT outcome FROM fetch_attempts
        WHERE race_id = ? AND race_id != -1 AND outcome = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (race_id, FetchOutcome.OK),
    ).fetchone()

    # Resolve stale alert if the most recent real attempt is OK.
    if last_ok and active_alert_id:
        _resolve_alert(db, active_alert_id)
        logger.info("Resolved alert for race %s (latest fetch OK)", race_id)

    last = _last_attempt(db, race_id)
    if not last:
        return
    if not _is_actionable_failure(last["outcome"]):
        return
    if not _race_started_long_ago(race_date, now):
        return

    active_alert_id = _active_alert_for_race(db, race_id)
    if active_alert_id:
        logger.debug("Alert already active for race %s; suppressing repeat", race_id)
        return

    _send_alert(db, race_id, last)
