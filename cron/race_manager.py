#!/usr/bin/env python3
"""
F1 Race Manager — state-machine cron for race-weekend automation.

Run via K8s CronJob every 5 min on Fri/Sat/Sun/Mon.
When no races are active the script exits in < 1 s.

Stages per race (tracked in the race_stages table):

    (no entry) → watching   race is open and starts within 12 h
    watching   → locked     race start ≤ 6 min away (or already past)
    locked     → polling    1 h 30 min since lock
    polling    → completed  API returns full podium → scores calculated

Between race weekends nothing is active, so the script does two quick
DB queries and exits.

Results come from OpenF1 (src/openf1.py) — the same client the web app uses.
"""

import argparse
import os
import sys
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

# openf1.py lives in src/, a sibling of this cron/ directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import openf1
import fetch_attempts
from fetch_attempts import FetchFailure, Outcome

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('race_manager')

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))

WATCH_WINDOW = timedelta(hours=12)
LOCK_LEAD = timedelta(minutes=6)
POLL_DELAY = timedelta(hours=1, minutes=30)
POLL_INTERVAL = timedelta(minutes=5)
MAX_POLL_DURATION = timedelta(hours=6)

ISO_FMT = '%Y-%m-%dT%H:%M:%SZ'


# ── helpers ─────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _utcnow():
    return datetime.now(timezone.utc)


def _parse_dt(s):
    if not s:
        return None
    s = s.strip().replace('Z', '')
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _now_iso(now):
    return now.strftime(ISO_FMT)


def ensure_stage_table(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS race_stages (
            race_id      INTEGER PRIMARY KEY,
            stage        TEXT    NOT NULL
                         CHECK (stage IN ('watching','locked','polling','completed')),
            entered_at   TEXT    NOT NULL,
            last_poll_at TEXT,
            poll_count   INTEGER DEFAULT 0,
            FOREIGN KEY (race_id) REFERENCES races(id)
        )
    ''')
    db.commit()


# ── (no entry) → watching ──────────────────────────────────────────

def promote_to_watching(db, now):
    """Detect open races starting within WATCH_WINDOW."""
    cutoff = now + WATCH_WINDOW
    rows = db.execute('''
        SELECT r.id, r.name, r.date
        FROM races r
        LEFT JOIN race_stages rs ON r.id = rs.race_id
        WHERE r.status = 'open' AND rs.race_id IS NULL
        ORDER BY r.date
    ''').fetchall()
    for r in rows:
        race_dt = _parse_dt(r['date'])
        if race_dt and race_dt <= cutoff:
            db.execute(
                'INSERT OR IGNORE INTO race_stages (race_id, stage, entered_at) VALUES (?, ?, ?)',
                (r['id'], 'watching', _now_iso(now)),
            )
            logger.info("→ watching  %s (starts %s)", r['name'], r['date'])
    db.commit()


# ── watching → locked ───────────────────────────────────────────────

def promote_to_locked(db, now):
    """Lock voting when race start is ≤ LOCK_LEAD away (or already past)."""
    rows = db.execute('''
        SELECT rs.race_id, r.name, r.date
        FROM race_stages rs
        JOIN races r ON r.id = rs.race_id
        WHERE rs.stage = 'watching'
    ''').fetchall()
    for r in rows:
        race_dt = _parse_dt(r['date'])
        if not race_dt:
            continue
        if race_dt - LOCK_LEAD <= now:
            db.execute(
                "UPDATE races SET status = 'locked' WHERE id = ? AND status = 'open'",
                (r['race_id'],),
            )
            db.execute(
                "UPDATE race_stages SET stage = 'locked', entered_at = ? WHERE race_id = ?",
                (_now_iso(now), r['race_id']),
            )
            logger.info("→ locked    %s (started %s)", r['name'], r['date'])
    db.commit()


# ── locked → polling ────────────────────────────────────────────────

def promote_to_polling(db, now):
    """Begin polling once POLL_DELAY has elapsed since lock."""
    rows = db.execute('''
        SELECT rs.race_id, r.name, rs.entered_at
        FROM race_stages rs
        JOIN races r ON r.id = rs.race_id
        WHERE rs.stage = 'locked'
    ''').fetchall()
    for r in rows:
        locked_at = _parse_dt(r['entered_at'])
        if locked_at and now >= locked_at + POLL_DELAY:
            db.execute('''
                UPDATE race_stages
                SET stage = 'polling', entered_at = ?, last_poll_at = NULL, poll_count = 0
                WHERE race_id = ?
            ''', (_now_iso(now), r['race_id']))
            logger.info("→ polling   %s (locked at %s)", r['name'], r['entered_at'])
    db.commit()


# ── polling → completed ────────────────────────────────────────────

def _resolve_session_key(db, race_id, round_num, season, session_key=None):
    """
    session_key for a race, resolving lazily for rows seeded before the
    column existed. Mirrors src/app.py's _race_session_key.
    """
    if session_key:
        return session_key
    cache_key = f"sessions?year={season}&session_type=Race"
    try:
        sessions = openf1.get_race_sessions(season=season, db=db).data
    except openf1.OpenF1Error as e:
        fetch_attempts.record_fetch_attempt(
            db, 'sessions', cache_key, fetch_attempts.outcome_for_error(e),
            http_status=getattr(e, 'status_code', None), race_id=race_id, detail=str(e),
        )
        logger.warning("Could not resolve session_key for race %s: %s", race_id, e)
        return None
    fetch_attempts.record_fetch_attempt(
        db, 'sessions', cache_key, Outcome.OK if sessions else Outcome.EMPTY, race_id=race_id,
    )
    for session_data in sessions:
        if session_data.get('round') == round_num:
            db.execute('UPDATE races SET session_key = ? WHERE id = ?',
                       (session_data['session_key'], race_id))
            db.commit()
            return session_data['session_key']
    return None


def _get_driver_id_by_number(db, driver_number):
    """DB driver id from a car number (how OpenF1 identifies drivers)."""
    if driver_number is None:
        return None
    row = db.execute('SELECT id FROM drivers WHERE number = ?', (driver_number,)).fetchone()
    return row['id'] if row else None


def _fetch_podium(db, session_key, race_id=None, race_date=None):
    endpoint = 'session_result'
    cache_key = f"session_result?session_key={session_key}"
    try:
        podium = openf1.get_podium(session_key, db=db)
    except openf1.OpenF1Error as e:
        outcome = fetch_attempts.outcome_for_error(e)
        fetch_attempts.record_fetch_attempt(
            db, endpoint, cache_key, outcome, http_status=getattr(e, 'status_code', None),
            session_key=session_key, race_id=race_id, detail=str(e),
        )
        logger.error("OpenF1 request failed for session %s: %s", session_key, e)
        raise FetchFailure(f"OpenF1 request failed for session {session_key}: {e}", outcome) from e

    if not podium:
        finished_long_ago = fetch_attempts.race_finished_long_ago(race_date) if race_date else False
        fetch_attempts.record_fetch_attempt(
            db, endpoint, cache_key, Outcome.EMPTY, session_key=session_key, race_id=race_id,
            detail='race finished >4h ago' if finished_long_ago else None,
        )
        if finished_long_ago:
            msg = f"No podium for session {session_key} more than 4h after race start"
            logger.error(msg)
            raise FetchFailure(msg, Outcome.EMPTY)
        return None

    fetch_attempts.record_fetch_attempt(
        db, endpoint, cache_key, Outcome.OK, session_key=session_key, race_id=race_id,
    )
    resolved = {}
    for slot in ('p1', 'p2', 'p3'):
        driver_db_id = _get_driver_id_by_number(db, podium[slot]['driver_number'])
        if driver_db_id is None:
            logger.error("driver #%s (%s) not in drivers table — skipping ingest",
                         podium[slot]['driver_number'], podium[slot]['driver_name'])
            return None
        resolved[slot] = {'driver_id': driver_db_id, 'driver_name': podium[slot]['driver_name']}
    return resolved


def _calculate_score(pred, res):
    pts = 0
    if pred['p1_driver_id'] == res['p1_driver_id']:
        pts += 10
    if pred['p2_driver_id'] == res['p2_driver_id']:
        pts += 6
    if pred['p3_driver_id'] == res['p3_driver_id']:
        pts += 4
    pred_set = {pred['p1_driver_id'], pred['p2_driver_id'], pred['p3_driver_id']}
    res_set = {res['p1_driver_id'], res['p2_driver_id'], res['p3_driver_id']}
    for did in pred_set & res_set:
        exact = (
            (did == pred['p1_driver_id'] and did == res['p1_driver_id'])
            or (did == pred['p2_driver_id'] and did == res['p2_driver_id'])
            or (did == pred['p3_driver_id'] and did == res['p3_driver_id'])
        )
        if not exact:
            pts += 1
    return pts


def _fetch_safety_car_summary(db, session_key):
    """Best-effort safety-car facts for the race (F1-07).

    Failure here must never block podium/score ingestion — an upstream
    race_control hiccup is not a reason to fail the whole poll.
    """
    if session_key is None:
        return None
    try:
        return openf1.get_safety_car_summary(session_key, db=db)
    except openf1.OpenF1Error as e:
        logger.warning("Could not fetch safety-car summary for session %s: %s", session_key, e)
        return None


def _save_results_and_score(db, race_id, podium, session_key=None):
    p1 = podium['p1']['driver_id']
    p2 = podium['p2']['driver_id']
    p3 = podium['p3']['driver_id']
    sc = _fetch_safety_car_summary(db, session_key) or {}

    db.execute('''
        INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id,
                             had_safety_car, safety_car_count,
                             had_virtual_safety_car, virtual_safety_car_count,
                             data_source, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'openf1', CURRENT_TIMESTAMP)
        ON CONFLICT(race_id) DO UPDATE SET
            p1_driver_id = excluded.p1_driver_id,
            p2_driver_id = excluded.p2_driver_id,
            p3_driver_id = excluded.p3_driver_id,
            had_safety_car = excluded.had_safety_car,
            safety_car_count = excluded.safety_car_count,
            had_virtual_safety_car = excluded.had_virtual_safety_car,
            virtual_safety_car_count = excluded.virtual_safety_car_count,
            data_source = excluded.data_source,
            recorded_at = excluded.recorded_at
    ''', (race_id, p1, p2, p3,
          1 if sc.get('had_safety_car') else 0, sc.get('safety_car_count'),
          1 if sc.get('had_virtual_safety_car') else 0, sc.get('virtual_safety_car_count')))

    db.execute("UPDATE races SET status = 'completed' WHERE id = ?", (race_id,))

    res = {'p1_driver_id': p1, 'p2_driver_id': p2, 'p3_driver_id': p3}
    for pred in db.execute('SELECT * FROM predictions WHERE race_id = ?', (race_id,)).fetchall():
        pts = _calculate_score(dict(pred), res)
        db.execute('''
            INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)
            ON CONFLICT(user_id, race_id) DO UPDATE SET points = excluded.points
        ''', (pred['user_id'], race_id, pts))
        logger.info("  user %s → %d pts", pred['user_id'][:8], pts)

    db.commit()
    return True


def poll_for_results(db, now):
    rows = db.execute('''
        SELECT rs.race_id, rs.entered_at, rs.last_poll_at, rs.poll_count,
               r.name, r.round, r.date, r.session_key
        FROM race_stages rs
        JOIN races r ON r.id = rs.race_id
        WHERE rs.stage = 'polling'
    ''').fetchall()

    failures = []

    for r in rows:
        started = _parse_dt(r['entered_at'])
        if started and now > started + MAX_POLL_DURATION:
            logger.warning("max poll time exceeded for %s — giving up", r['name'])
            db.execute(
                "UPDATE race_stages SET stage = 'completed', entered_at = ? WHERE race_id = ?",
                (_now_iso(now), r['race_id']),
            )
            db.commit()
            continue

        last = _parse_dt(r['last_poll_at'])
        if last and now < last + POLL_INTERVAL:
            continue

        logger.info("poll #%d   %s (round %d)", r['poll_count'] + 1, r['name'], r['round'])
        session_key = _resolve_session_key(db, r['race_id'], r['round'], F1_SEASON, r['session_key'])
        podium = None
        if session_key:
            try:
                podium = _fetch_podium(db, session_key, race_id=r['race_id'], race_date=r['date'])
            except FetchFailure as e:
                logger.error("❌ Fetch failed for %s: %s", r['name'], e)
                failures.append(r['name'])
                db.execute('''
                    UPDATE race_stages SET last_poll_at = ?, poll_count = poll_count + 1
                    WHERE race_id = ?
                ''', (_now_iso(now), r['race_id']))
                db.commit()
                continue

        if podium:
            ok = _save_results_and_score(db, r['race_id'], podium, session_key=session_key)
            if ok:
                db.execute('''
                    UPDATE race_stages
                    SET stage = 'completed', entered_at = ?, last_poll_at = ?, poll_count = poll_count + 1
                    WHERE race_id = ?
                ''', (_now_iso(now), _now_iso(now), r['race_id']))
                logger.info("→ completed %s", r['name'])
        else:
            db.execute('''
                UPDATE race_stages SET last_poll_at = ?, poll_count = poll_count + 1
                WHERE race_id = ?
            ''', (_now_iso(now), r['race_id']))
            logger.info("  no results yet for %s", r['name'])
        db.commit()

    return failures


# ── status command ──────────────────────────────────────────────────

def show_status(db):
    ensure_stage_table(db)
    rows = db.execute('''
        SELECT rs.*, r.name, r.round, r.date, r.status AS race_status
        FROM race_stages rs
        JOIN races r ON r.id = rs.race_id
        ORDER BY r.date
    ''').fetchall()
    if not rows:
        print("No race stages tracked yet.")
        return
    for r in rows:
        print(f"  round {r['round']:>2}  {r['name']:<30}  stage={r['stage']:<10}  "
              f"entered={r['entered_at']}  polls={r['poll_count']}")


# ── main ────────────────────────────────────────────────────────────

def _test_api():
    """Smoke-test OpenF1 connectivity (most recent Race session of F1_SEASON)."""
    try:
        sessions = openf1.get_race_sessions(season=F1_SEASON).data
    except openf1.OpenF1Error as e:
        logger.error("Test fetch failed: %s", e)
        return 1
    if not sessions:
        logger.error("Test fetch failed: no sessions available")
        return 1
    session_key = sessions[-1]['session_key']
    try:
        podium = openf1.get_podium(session_key)
    except openf1.OpenF1Error as e:
        logger.error("Test fetch failed: %s", e)
        return 1
    if podium:
        logger.info(
            "OK P1=%s P2=%s P3=%s",
            podium['p1']['driver_name'],
            podium['p2']['driver_name'],
            podium['p3']['driver_name'],
        )
        return 0
    logger.error("Test fetch failed")
    return 1


def main():
    parser = argparse.ArgumentParser(description='F1 race weekend state machine')
    parser.add_argument('--status', action='store_true', help='Show current race stages')
    parser.add_argument('--test-api', action='store_true',
                        help='Smoke-test OpenF1 connectivity')
    args = parser.parse_args()

    if args.test_api:
        sys.exit(_test_api())

    if not os.path.exists(DATABASE_PATH):
        logger.error("DB not found at %s", DATABASE_PATH)
        sys.exit(1)

    db = get_db()
    ensure_stage_table(db)

    if args.status:
        show_status(db)
        db.close()
        return

    now = _utcnow()

    promote_to_watching(db, now)
    promote_to_locked(db, now)
    promote_to_polling(db, now)
    failures = poll_for_results(db, now)

    db.close()

    if failures:
        logger.error("Race manager fetch failed for: %s", ', '.join(failures))
        sys.exit(1)


if __name__ == '__main__':
    main()
