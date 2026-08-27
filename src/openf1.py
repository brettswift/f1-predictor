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
import random
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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

# Offline mode: never touch the network, serve cache only. Used by the test
# suite (tests must be deterministic and network-free) and available as an
# operational kill switch if upstream needs to be left alone.
OFFLINE = os.environ.get("OPENF1_OFFLINE", "false").lower() == "true"

# --------------------------------------------------------------------------
# Rate-limit handling (F1-08 / BUD-167)
# --------------------------------------------------------------------------

# Base for exponential backoff on a 429 with no Retry-After header. Doubles
# per attempt (attempt 1: base, attempt 2: 2*base, ...) with full jitter.
RATE_LIMIT_BACKOFF_BASE_SEC = float(os.environ.get("OPENF1_RATE_LIMIT_BACKOFF_BASE_SEC", "2.0"))

# Total time a single _get() call may spend asleep waiting out 429s. The
# race-manager CronJob (base/race-manager-cronjob.yaml) runs on "*/5 * * * *"
# (every 5 min / 300s) with concurrencyPolicy: Forbid, so a rate-limited call
# that keeps honoring a large Retry-After can block the next scheduled run.
# 60s leaves generous headroom under that window and still lets one
# reasonable Retry-After (e.g. 30s) be honored in full.
RATE_LIMIT_MAX_WAIT_SEC = float(os.environ.get("OPENF1_RATE_LIMIT_MAX_WAIT_SEC", "60"))

# Process-wide minimum spacing between outbound requests — a token-bucket-
# of-one so a dev loop or test run can't burst past OpenF1's limit the way
# prod's fixed */5 cadence never does. The OpenF1 maintainer states
# anonymous access is capped at "30 [requests] every 10 seconds"
# (github.com/br-g/openf1 issue #113, comment 2024-10-26) — ~3 req/s, i.e.
# ~0.33s apart. That's short enough not to add any real delay to the
# handful of sequential requests one production cron run makes.
MIN_REQUEST_INTERVAL_SEC = float(os.environ.get("OPENF1_MIN_REQUEST_INTERVAL_SEC", str(10 / 30)))

_rate_limiter_lock = threading.Lock()
_last_request_monotonic: Optional[float] = None


class OpenF1Error(RuntimeError):
    """
    Upstream failed and no usable cached payload was available.

    Carries just enough about the underlying failure (status_code, is_timeout)
    for callers to classify the outcome for observability (BUD-125) without
    parsing the message string. status_code == 429 is the marker BUD-125's
    `fetch_attempts.outcome_for_error()` uses to record `rate_limited` instead
    of the generic `http_error` — a 429 gets its own backoff/budget in
    `_get()`'s retry loop (F1-08 / BUD-167), distinct from the linear backoff
    used for 5xx/timeouts.
    """

    def __init__(self, message: str, *, status_code: Optional[int] = None,
                 is_timeout: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.is_timeout = is_timeout


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

def _is_rate_limited(response: Optional["requests.Response"]) -> bool:
    """
    429 is unambiguous. OpenF1 has also been observed returning 403 for quota
    exhaustion, so treat a 403 as rate-limited too, but only when the
    response itself signals quota (a Retry-After header, or "quota"/"rate
    limit" in the body) — a plain 403 (e.g. a genuinely forbidden endpoint)
    must not be misclassified as rate limiting.
    """
    if response is None:
        return False
    if response.status_code == 429:
        return True
    if response.status_code == 403:
        if "Retry-After" in response.headers:
            return True
        body = (response.text or "").lower()
        if "quota" in body or "rate limit" in body:
            return True
    return False


def _parse_retry_after(response: "requests.Response") -> Optional[float]:
    """Retry-After per RFC 9110: either delay-seconds or an HTTP-date."""
    value = response.headers.get("Retry-After")
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


def _rate_limit_wait_seconds(response: "requests.Response", attempt: int) -> tuple[float, str]:
    """
    How long to back off before retrying a rate-limited request, and why.

    Honors Retry-After when the server sends one. Otherwise backs off
    exponentially (doubling per attempt) with full jitter — not the linear
    1.5s*attempt used for a general transport failure — so repeated retries
    spread out instead of arriving in lockstep.
    """
    retry_after = _parse_retry_after(response)
    if retry_after is not None:
        return retry_after, "Retry-After header"
    base = RATE_LIMIT_BACKOFF_BASE_SEC * (2 ** (attempt - 1))
    return random.uniform(0, base), "exponential backoff with jitter"


def _throttle() -> None:
    """
    Process-wide minimum spacing between outbound OpenF1 requests. A no-op
    once the previous request is far enough in the past — at production's
    */5 cadence that's every time, so this never adds latency there. Only a
    tight loop (dev, tests, a retry burst) ever actually waits here.
    """
    global _last_request_monotonic
    if MIN_REQUEST_INTERVAL_SEC <= 0:
        return
    with _rate_limiter_lock:
        now = time.monotonic()
        if _last_request_monotonic is not None:
            wait = MIN_REQUEST_INTERVAL_SEC - (now - _last_request_monotonic)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
        _last_request_monotonic = now


def _get(path: str, params: Optional[dict] = None,
         db: Optional[sqlite3.Connection] = None) -> CachedResult:
    """
    GET an OpenF1 endpoint, with retries, then last-known-good cache fallback.

    A 429 (or quota-flavored 403) is handled distinctly from a general
    transport failure: it honors Retry-After (or backs off exponentially
    with jitter when absent), bounded by RATE_LIMIT_MAX_WAIT_SEC so a
    rate-limited call fails fast and yields to the next scheduled cron run
    rather than stacking requests inside one run. Either way, exhausting
    retries falls through to the last-known-good cache exactly the same —
    being rate-limited is never worse than an outage.

    Raises OpenF1Error only when upstream failed *and* nothing is cached —
    that is the one case where the caller genuinely has no data to show.
    """
    params = params or {}
    url = f"{OPENF1_BASE_URL}/{path.lstrip('/')}"
    key = _cache_key(path, params)
    last_exc: Optional[Exception] = None
    rate_limited = False
    rate_limit_wait_used = 0.0

    if OFFLINE:
        cached = _cache_read(db, key) if db is not None else None
        if cached is not None:
            return cached
        raise OpenF1Error(f"OPENF1_OFFLINE is set and {key} is not cached")

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        _throttle()
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
            if db is not None:
                _cache_write(db, key, data)
            return CachedResult(data=data, fetched_at=datetime.now(timezone.utc))
        except requests.HTTPError as exc:
            last_exc = exc
            if _is_rate_limited(exc.response):
                rate_limited = True
                sleep_for, source = _rate_limit_wait_seconds(exc.response, attempt)
                remaining_budget = RATE_LIMIT_MAX_WAIT_SEC - rate_limit_wait_used
                if attempt >= RETRY_ATTEMPTS or sleep_for > remaining_budget:
                    logger.error(
                        "OpenF1 %s rate-limited (HTTP %s) — giving up after %d attempt(s), "
                        "%.1fs/%.1fs retry budget used: %s",
                        key, exc.response.status_code, attempt,
                        rate_limit_wait_used, RATE_LIMIT_MAX_WAIT_SEC, exc,
                    )
                    break
                logger.warning(
                    "OpenF1 %s rate-limited (HTTP %s, attempt %d/%d) — backing off %.1fs (%s)",
                    key, exc.response.status_code, attempt, RETRY_ATTEMPTS, sleep_for, source,
                )
                rate_limit_wait_used += sleep_for
                time.sleep(sleep_for)
            else:
                rate_limited = False
                if attempt < RETRY_ATTEMPTS:
                    sleep_for = RETRY_BACKOFF_SEC * attempt
                    logger.warning("OpenF1 %s failed (attempt %d/%d): %s — retrying in %.1fs",
                                   key, attempt, RETRY_ATTEMPTS, exc, sleep_for)
                    time.sleep(sleep_for)
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            rate_limited = False
            if attempt < RETRY_ATTEMPTS:
                sleep_for = RETRY_BACKOFF_SEC * attempt
                logger.warning("OpenF1 %s failed (attempt %d/%d): %s — retrying in %.1fs",
                               key, attempt, RETRY_ATTEMPTS, exc, sleep_for)
                time.sleep(sleep_for)

    if rate_limited:
        logger.error("OpenF1 %s exhausted rate-limit retry budget (%.1fs used): %s",
                      key, rate_limit_wait_used, last_exc)
    else:
        logger.error("OpenF1 %s failed after %d attempts: %s", key, RETRY_ATTEMPTS, last_exc)

    if db is not None:
        cached = _cache_read(db, key)
        if cached is not None:
            logger.warning("Serving cached %s from %s (age %s)", key, cached.fetched_at, cached.age_label())
            return cached

    status_code = None
    is_timeout = isinstance(last_exc, requests.Timeout)
    if isinstance(last_exc, requests.HTTPError) and last_exc.response is not None:
        status_code = last_exc.response.status_code
    if rate_limited:
        raise OpenF1Error(
            f"OpenF1 rate limit exceeded and no cache available: {key}: {last_exc}",
            status_code=status_code,
            is_timeout=is_timeout,
        )
    raise OpenF1Error(
        f"OpenF1 request failed and no cache available: {key}: {last_exc}",
        status_code=status_code,
        is_timeout=is_timeout,
    )


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
#
# Each intervention produces a deploy/end pair, verified against 2026 data:
#   "SAFETY CAR DEPLOYED" -> "SAFETY CAR IN THIS LAP"  (the car comes IN)
#   "VSC DEPLOYED"        -> "VSC ENDING"
# Only the deploy side is counted, so one intervention counts once.
_SC_DEPLOY_TOKENS = ("SAFETY CAR DEPLOYED",)
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
