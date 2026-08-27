#!/usr/bin/env python3
"""
F1 Race Results Fetcher - Cron job (hourly in cluster).
Fetches race results from OpenF1 (same client as the web app, src/openf1.py).
"""

import argparse
import os
import sys
import sqlite3
import logging

# openf1.py lives in src/, a sibling of this cron/ directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import openf1
import fetch_attempts
from fetch_attempts import FetchFailure, Outcome

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path (matches the app's config)
DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def auto_lock_past_races():
    """Lock races whose start time has passed (same logic as app auto_lock_races)."""
    db = get_db()
    try:
        cur = db.execute('''
            UPDATE races SET status = 'locked'
            WHERE status = 'open' AND datetime(date) < datetime('now')
        ''')
        db.commit()
        if cur.rowcount:
            logger.info(f"Auto-locked {cur.rowcount} race(s) whose start time has passed")
    finally:
        db.close()


def get_locked_races_without_results():
    """Get races that are locked but don't have results yet."""
    db = get_db()
    try:
        races = db.execute('''
            SELECT r.id, r.name, r.round, r.date, r.session_key
            FROM races r
            LEFT JOIN results res ON r.id = res.race_id
            WHERE r.status = 'locked' AND res.race_id IS NULL
            ORDER BY r.date ASC
        ''').fetchall()
        return [dict(race) for race in races]
    finally:
        db.close()


def _resolve_session_key(db, race, season):
    """
    session_key for a race, resolving lazily for rows seeded before the
    column existed. Mirrors src/app.py's _race_session_key.
    """
    key = race.get('session_key')
    if key:
        return key
    cache_key = f"sessions?year={season}&session_type=Race"
    try:
        sessions = openf1.get_race_sessions(season=season, db=db).data
    except openf1.OpenF1Error as e:
        fetch_attempts.record_fetch_attempt(
            db, 'sessions', cache_key, fetch_attempts.outcome_for_error(e),
            http_status=getattr(e, 'status_code', None), race_id=race['id'], detail=str(e),
        )
        logger.warning(f"Could not resolve session_key for race {race['id']}: {e}")
        return None
    fetch_attempts.record_fetch_attempt(
        db, 'sessions', cache_key, Outcome.OK if sessions else Outcome.EMPTY, race_id=race['id'],
    )
    for session_data in sessions:
        if session_data.get('round') == race['round']:
            db.execute('UPDATE races SET session_key = ? WHERE id = ?',
                       (session_data['session_key'], race['id']))
            db.commit()
            return session_data['session_key']
    return None


def get_driver_id_by_number(db, driver_number):
    """DB driver id from a car number (how OpenF1 identifies drivers)."""
    if driver_number is None:
        return None
    row = db.execute('SELECT id FROM drivers WHERE number = ?', (driver_number,)).fetchone()
    return row['id'] if row else None


def fetch_race_results_from_api(db, race, season=None):
    """
    Podium for a race from OpenF1, as DB driver ids.

    Returns None when the race has not finished yet, its session_key cannot
    be resolved, or the podium drivers cannot be matched — all of which mean
    "try again next run" rather than an error.

    Raises FetchFailure when OpenF1 failed after exhausting retries, or
    returned no podium for a race that finished more than 4 hours ago — both
    of which should fail the cron Job (BUD-125) rather than be swallowed.
    """
    season = season or F1_SEASON
    session_key = _resolve_session_key(db, race, season)
    if not session_key:
        return None

    endpoint = 'session_result'
    cache_key = f"session_result?session_key={session_key}"
    try:
        podium = openf1.get_podium(session_key, db=db)
    except openf1.OpenF1Error as e:
        outcome = fetch_attempts.outcome_for_error(e)
        fetch_attempts.record_fetch_attempt(
            db, endpoint, cache_key, outcome, http_status=getattr(e, 'status_code', None),
            session_key=session_key, race_id=race['id'], detail=str(e),
        )
        logger.error(f"OpenF1 request failed: {e}")
        raise FetchFailure(f"OpenF1 request failed for {race['name']}: {e}", outcome) from e

    if not podium:
        finished_long_ago = fetch_attempts.race_finished_long_ago(race.get('date'))
        fetch_attempts.record_fetch_attempt(
            db, endpoint, cache_key, Outcome.EMPTY, session_key=session_key, race_id=race['id'],
            detail='race finished >4h ago' if finished_long_ago else None,
        )
        if finished_long_ago:
            msg = f"No podium for {race['name']} more than 4h after it finished"
            logger.error(msg)
            raise FetchFailure(msg, Outcome.EMPTY)
        logger.info(f"Race not complete yet for {race['name']}")
        return None

    fetch_attempts.record_fetch_attempt(
        db, endpoint, cache_key, Outcome.OK, session_key=session_key, race_id=race['id'],
    )
    resolved = {}
    for slot in ('p1', 'p2', 'p3'):
        driver_db_id = get_driver_id_by_number(db, podium[slot]['driver_number'])
        if driver_db_id is None:
            logger.error(
                "Race %s: driver #%s (%s) not in drivers table — skipping ingest",
                race['id'], podium[slot]['driver_number'], podium[slot]['driver_name'])
            return None
        resolved[slot] = {
            'position': podium[slot]['position'],
            'driver_id': driver_db_id,
            'driver_name': podium[slot]['driver_name'],
        }

    logger.info(f"Fetched podium: P1={resolved['p1']['driver_name']}, "
               f"P2={resolved['p2']['driver_name']}, "
               f"P3={resolved['p3']['driver_name']}")
    return resolved


def calculate_score(prediction, result):
    """Calculate score for a prediction against actual results."""
    points = 0

    # Exact positions
    if prediction['p1_driver_id'] == result['p1_driver_id']:
        points += 10
    if prediction['p2_driver_id'] == result['p2_driver_id']:
        points += 6
    if prediction['p3_driver_id'] == result['p3_driver_id']:
        points += 4

    # Driver in top 3 but wrong position (1 point each)
    pred_drivers = {prediction['p1_driver_id'], prediction['p2_driver_id'], prediction['p3_driver_id']}
    result_drivers = {result['p1_driver_id'], result['p2_driver_id'], result['p3_driver_id']}

    for driver_id in pred_drivers:
        if driver_id in result_drivers:
            # Check if exact position was already counted
            is_exact = (
                (driver_id == prediction['p1_driver_id'] and driver_id == result['p1_driver_id']) or
                (driver_id == prediction['p2_driver_id'] and driver_id == result['p2_driver_id']) or
                (driver_id == prediction['p3_driver_id'] and driver_id == result['p3_driver_id'])
            )
            if not is_exact:
                points += 1

    return points


def update_race_results(race_id, podium):
    """Update database with race results and calculate scores."""
    db = get_db()
    try:
        p1_id = podium['p1']['driver_id']
        p2_id = podium['p2']['driver_id']
        p3_id = podium['p3']['driver_id']

        # Insert results
        db.execute('''
            INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(race_id) DO UPDATE SET
                p1_driver_id = excluded.p1_driver_id,
                p2_driver_id = excluded.p2_driver_id,
                p3_driver_id = excluded.p3_driver_id
        ''', (race_id, p1_id, p2_id, p3_id))

        # Update race status to completed
        db.execute(
            "UPDATE races SET status = 'completed' WHERE id = ?",
            (race_id,)
        )

        # Calculate scores for all predictions
        predictions = db.execute(
            'SELECT * FROM predictions WHERE race_id = ?',
            (race_id,)
        ).fetchall()

        result_data = {
            'p1_driver_id': p1_id,
            'p2_driver_id': p2_id,
            'p3_driver_id': p3_id
        }

        for pred in predictions:
            pred_dict = dict(pred)
            points = calculate_score(pred_dict, result_data)

            db.execute('''
                INSERT INTO scores (user_id, race_id, points)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, race_id) DO UPDATE SET
                    points = excluded.points
            ''', (pred['user_id'], race_id, points))

            logger.info(f"User {pred['user_id']} scored {points} points for race {race_id}")

        db.commit()
        logger.info(f"Race {race_id} completed and scores calculated")
        return True

    except Exception as e:
        logger.error(f"Failed to update race results: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def run_test_api_fetch():
    """Hit OpenF1 (most recent Race session) to verify connectivity and parsing."""
    logger.info(f"Test fetch: {openf1.OPENF1_BASE_URL} (season {F1_SEASON})")
    try:
        sessions = openf1.get_race_sessions(season=F1_SEASON).data
    except openf1.OpenF1Error as e:
        logger.error(f"Test fetch failed: {e}")
        return 1
    if not sessions:
        logger.error("Test fetch failed: no sessions available")
        return 1
    session_key = sessions[-1]['session_key']
    try:
        podium = openf1.get_podium(session_key)
    except openf1.OpenF1Error as e:
        logger.error(f"Test fetch failed: {e}")
        return 1
    if podium:
        logger.info(
            f"OK P1={podium['p1']['driver_name']} P2={podium['p2']['driver_name']} "
            f"P3={podium['p3']['driver_name']}"
        )
        return 0
    logger.error("Test fetch failed or incomplete results")
    return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Fetch F1 race results into predictor DB')
    parser.add_argument(
        '--test-api',
        action='store_true',
        help='Only verify API connectivity; no database access',
    )
    args = parser.parse_args()
    if args.test_api:
        sys.exit(run_test_api_fetch())

    logger.info("Starting race results fetcher (OpenF1 %s, season %s)", openf1.OPENF1_BASE_URL, F1_SEASON)

    # Check if database exists
    if not os.path.exists(DATABASE_PATH):
        logger.error(f"Database not found at {DATABASE_PATH}")
        sys.exit(1)

    # Lock past races first so hourly job can fetch even if nobody opened the site
    auto_lock_past_races()

    # Get locked races without results
    races = get_locked_races_without_results()

    if not races:
        logger.info("No locked races awaiting results")
        sys.exit(0)

    failures = []

    for race in races:
        logger.info(f"Checking race {race['round']}: {race['name']}")

        db = get_db()
        try:
            podium = fetch_race_results_from_api(db, race)
        except FetchFailure as e:
            logger.error(f"❌ Fetch failed for {race['name']}: {e}")
            failures.append(race['name'])
            continue
        finally:
            db.close()

        if podium:
            success = update_race_results(race['id'], podium)
            if success:
                logger.info(f"✅ Successfully updated results for {race['name']}")
            else:
                logger.error(f"❌ Failed to update results for {race['name']}")
        else:
            logger.info(f"⏳ Results not available yet for {race['name']}")

    if failures:
        logger.error(f"Race results fetcher failed for: {', '.join(failures)}")
        sys.exit(1)

    logger.info("Race results fetcher completed")


if __name__ == '__main__':
    main()
