#!/usr/bin/env python3
"""
OpenF1 API client — the single place this app talks to an upstream F1 data source.

Replaces the previous Jolpica/Ergast integration, which was duplicated across
src/app.py, cron/fetch_race_results.py, cron/race_manager.py and
cron/refresh_drivers.py (four copies of URL building and response parsing).

Design notes (F1-01, F1-02):
  * Everything upstream goes through `_get()`. Adding retries, caching or
    swapping providers happens in one function, not four files.
  * Every successful read is written to a last-known-good cache table. When
    upstream fails, we serve the cached payload and report its age rather than
    surfacing an error to the user. `CachedResult.is_stale` lets callers
    decide whether to show a data-age indicator.
  * OpenF1 models a race as a `session` (session_type="Race") belonging to a
    `meeting` (the GP weekend). Results are keyed by `session_key` and identify
    drivers by `driver_number`, so podium lookups join session_result -> drivers.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

OPENF1_BASE_URL = os.environ.get("OPENF1_API_URL", "https://api.openf1.org/v1").rstrip("/")
F1_SEASON = int(os.environ.get("F1_SEASON", "2026"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("OPENF1_TIMEOUT_SEC", "30"))
RETRY_ATTEMPTS = int(os.environ.get("OPENF1_RETRY_ATTEMPTS", "3"))
RETRY_BACKOFF_SEC = float(os.environ.get("OPENF1_RETRY_BACKOFF_SEC", "1.5"))

# A cached payload older than this is flagged stale so the UI can say so.
CACHE_STALE_AFTER_SEC = int(os.environ.get("OPENF1_CACHE_STALE_SEC", str(6 * 3600)))


class OpenF1Error(RuntimeError):
    """Upstream failed and no usable cached payload was available."""


@dataclass
class CachedResult:
    """An upstream payload plus provenance, so callers can be honest about age."""

    data: Any
    fetched_at: datetime
    from_cache: bool = False

    @property
    def age_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.fetched_at).total_seconds()

    @property
    def is_stale(self) -> bool:
        return self.from_cache and self.age_seconds > CACHE_STALE_AFTER_SEC

    def age_label(self) -> str:
        """Human-readable age, for the data-age indicator in templates."""
        secs = int(self.age_seconds)
        if secs < 90:
            return "just now"
        mins = secs // 60
        if mins < 60:
            return f"{mins} min ago"
        hours = mins // 60
        if hours < 24:
            return f"{hours}h ago"
        return f"{hours // 24}d ago"


# --------------------------------------------------------------------------
# Last-known-good cache (F1-02)
# --------------------------------------------------------------------------

def ensure_cache_table(db: sqlite3.Connection) -> None:
    """Create the cache table. Safe to call repeatedly (used by init_db)."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS api_cache (
            cache_key   TEXT PRIMARY KEY,
            payload     TEXT NOT NULL,
            fetched_at  TIMESTAMP NOT NULL
        )
        """
    )


def _cache_key(path: str, params: dict) -> str:
    stable = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return f"{path}?{stable}" if stable else path


def _cache_write(db: sqlite3.Connection, key: str, data: Any) -> None:
    try:
        ensure_cache_table(db)
        db.execute(
            "INSERT INTO api_cache (cache_key, payload, fetched_at) VALUES (?, ?, ?) "
            "ON CONFLICT(cache_key) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
            (key, json.dumps(data), datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
    except sqlite3.Error as exc:  # cache failures must never break a good read
        logger.warning("api_cache write failed for %s: %s", key, exc)


def _cache_read(db: sqlite3.Connection, key: str) -> Optional[CachedResult]:
    try:
        ensure_cache_table(db)
        row = db.execute(
            "SELECT payload, fetched_at FROM api_cache WHERE cache_key = ?", (key,)
        ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("api_cache read failed for %s: %s", key, exc)
        return None
    if not row:
        return None
    payload, fetched_at = (row["payload"], row["fetched_at"]) if isinstance(row, sqlite3.Row) else (row[0], row[1])
    try:
        stamp = datetime.fromisoformat(fetched_at)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return CachedResult(data=json.loads(payload), fetched_at=stamp, from_cache=True)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("api_cache entry for %s is corrupt: %s", key, exc)
        return None


# --------------------------------------------------------------------------
# Core request path — everything upstream goes through here
# --------------------------------------------------------------------------

def _get(path: str, params: Optional[dict] = None,
         db: Optional[sqlite3.Connection] = None) -> CachedResult:
    """
    GET an OpenF1 endpoint, with retries, then last-known-good cache fallback.

    Raises OpenF1Error only when upstream failed *and* nothing is cached —
    that is the one case where the caller genuinely has no data to show.
    """
    params = params or {}
    url = f"{OPENF1_BASE_URL}/{path.lstrip('/')}"
    key = _cache_key(path, params)
    last_exc: Optional[Exception] = None

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
            if db is not None:
                _cache_write(db, key, data)
            return CachedResult(data=data, fetched_at=datetime.now(timezone.utc))
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < RETRY_ATTEMPTS:
                sleep_for = RETRY_BACKOFF_SEC * attempt
                logger.warning("OpenF1 %s failed (attempt %d/%d): %s — retrying in %.1fs",
                               key, attempt, RETRY_ATTEMPTS, exc, sleep_for)
                time.sleep(sleep_for)

    logger.error("OpenF1 %s failed after %d attempts: %s", key, RETRY_ATTEMPTS, last_exc)
    if db is not None:
        cached = _cache_read(db, key)
        if cached is not None:
            logger.warning("Serving cached %s from %s (age %s)", key, cached.fetched_at, cached.age_label())
            return cached
    raise OpenF1Error(f"OpenF1 request failed and no cache available: {key}: {last_exc}")


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

def get_race_sessions(season: int = None, db=None) -> CachedResult:
    """All Race sessions for a season, ordered by start time (the calendar)."""
    season = season or F1_SEASON
    res = _get("sessions", {"year": season, "session_type": "Race"}, db=db)
    sessions = [s for s in res.data if not s.get("is_cancelled")]
    sessions.sort(key=lambda s: s.get("date_start") or "")
    # OpenF1 has no round number; position in the season calendar is the round.
    for idx, s in enumerate(sessions, start=1):
        s["round"] = idx
    res.data = sessions
    return res


def get_meetings(season: int = None, db=None) -> CachedResult:
    """Meetings (GP weekends) — used for human-readable race names."""
    season = season or F1_SEASON
    return _get("meetings", {"year": season}, db=db)


def get_drivers(session_key: int = None, season: int = None, db=None) -> CachedResult:
    """
    Driver list. Prefer a session_key (grid for that race); fall back to the
    most recent session of the season when no key is given.
    """
    if session_key is not None:
        return _get("drivers", {"session_key": session_key}, db=db)
    sessions = get_race_sessions(season=season, db=db).data
    if not sessions:
        raise OpenF1Error("No sessions available to resolve a driver list")
    latest_started = [s for s in sessions if _has_started(s)]
    chosen = (latest_started or sessions)[-1 if latest_started else 0]
    return _get("drivers", {"session_key": chosen["session_key"]}, db=db)


def get_session_result(session_key: int, db=None) -> CachedResult:
    """Classification for a session, ordered by finishing position."""
    res = _get("session_result", {"session_key": session_key}, db=db)
    classified = [r for r in res.data if r.get("position") is not None]
    classified.sort(key=lambda r: r["position"])
    res.data = classified
    return res


def get_starting_grid(session_key: int, db=None) -> CachedResult:
    """Starting grid for a session."""
    return _get("starting_grid", {"session_key": session_key}, db=db)


def get_race_control(session_key: int, db=None) -> CachedResult:
    """Race control messages (flags, safety cars, investigations)."""
    return _get("race_control", {"session_key": session_key}, db=db)


# --------------------------------------------------------------------------
# Derived helpers
# --------------------------------------------------------------------------

def _has_started(session: dict) -> bool:
    start = session.get("date_start")
    if not start:
        return False
    try:
        return datetime.fromisoformat(start) <= datetime.now(timezone.utc)
    except ValueError:
        return False


def driver_display_name(driver: dict) -> str:
    """Human name for a driver record, tolerant of OpenF1's optional fields."""
    full = (driver.get("full_name") or "").strip()
    if full:
        # OpenF1 returns "Lando NORRIS" — normalise the shouted surname.
        parts = full.split()
        if len(parts) >= 2 and parts[-1].isupper():
            return " ".join(parts[:-1] + [parts[-1].title()])
        return full
    first = (driver.get("first_name") or "").strip()
    last = (driver.get("last_name") or "").strip()
    return f"{first} {last}".strip() or driver.get("name_acronym") or "Unknown"


def get_podium(session_key: int, db=None) -> Optional[dict]:
    """
    P1/P2/P3 for a completed race, with driver names resolved.

    Returns None when the race has not produced a full podium yet — callers
    treat that as "not finished", which is different from an upstream failure.
    """
    results = get_session_result(session_key, db=db).data
    if len(results) < 3:
        logger.info("Session %s has only %d classified results — not complete", session_key, len(results))
        return None

    drivers = {d.get("driver_number"): d for d in get_drivers(session_key=session_key, db=db).data}

    podium = {}
    for slot, row in zip(("p1", "p2", "p3"), results[:3]):
        driver = drivers.get(row.get("driver_number"), {})
        podium[slot] = {
            "position": row.get("position"),
            "driver_number": row.get("driver_number"),
            "driver_name": driver_display_name(driver) if driver else f"#{row.get('driver_number')}",
            "driver_code": driver.get("name_acronym") or "",
            "constructor": driver.get("team_name") or "",
        }
    return podium


# Messages OpenF1 emits in the SafetyCar category. "VSC" is a virtual safety
# car, which is a distinct thing from a full safety car and is counted apart.
_SC_DEPLOY_TOKENS = ("SAFETY CAR DEPLOYED", "SAFETY CAR IN THIS LAP")
_VSC_DEPLOY_TOKENS = ("VSC DEPLOYED",)


def get_safety_car_summary(session_key: int, db=None) -> dict:
    """
    Safety-car facts for a race (F1-07) — the data needed before safety-car
    predictions can be scored.

    Deployments are counted from "deployed" messages only; the paired
    "ending/in this lap" messages are ignored so one intervention counts once.
    """
    messages = get_race_control(session_key, db=db).data
    sc_msgs = [m for m in messages if (m.get("category") or "").lower() == "safetycar"]

    full_sc = 0
    virtual_sc = 0
    for m in sc_msgs:
        text = (m.get("message") or "").upper()
        if any(tok in text for tok in _VSC_DEPLOY_TOKENS):
            virtual_sc += 1
        elif any(tok in text for tok in _SC_DEPLOY_TOKENS):
            full_sc += 1

    return {
        "had_safety_car": full_sc > 0,
        "safety_car_count": full_sc,
        "had_virtual_safety_car": virtual_sc > 0,
        "virtual_safety_car_count": virtual_sc,
        # "any" is what a casual yes/no prediction means to a user.
        "had_any_safety_car": (full_sc + virtual_sc) > 0,
    }
