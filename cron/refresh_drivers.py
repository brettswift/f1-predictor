#!/usr/bin/env python3
"""
F1 Driver Refresh CronJob - Refreshes driver data from OpenF1 weekly.
Runs as a Kubernetes CronJob in the f1-predictor namespace.
"""

import argparse
import os
import sys
import sqlite3
import logging
from datetime import datetime

# openf1.py lives in src/, a sibling of this cron/ directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import openf1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_drivers_from_api(db=None):
    """Fetch the current driver grid from OpenF1 (same shape as src/app.py)."""
    try:
        drivers_raw = openf1.get_drivers(season=F1_SEASON, db=db).data
    except openf1.OpenF1Error as e:
        logger.error(f"Failed to fetch drivers from OpenF1: {e}")
        return None

    drivers = []
    for idx, driver in enumerate(drivers_raw, start=1):
        number = driver.get('driver_number')
        if number is None:
            continue
        name = openf1.driver_display_name(driver)
        drivers.append({
            'id': idx,
            # OpenF1 has no stable slug; derive one so existing driver_id
            # semantics (unique text key) still hold.
            'driver_id': (driver.get('name_acronym') or name).lower().replace(' ', '_'),
            'name': name,
            'number': int(number),
            'code': driver.get('name_acronym'),
            'nationality': driver.get('country_code'),
            'team': driver.get('team_name'),
        })

    if drivers:
        logger.info(f"Fetched {len(drivers)} drivers from OpenF1")
        return drivers
    return None


def refresh_drivers(db):
    """Refresh drivers in the database from API data.

    Preserves predictions by remapping driver IDs when possible.
    """
    logger.info("Starting driver refresh...")

    drivers = fetch_drivers_from_api(db)
    if not drivers:
        logger.error("Failed to fetch drivers from API")
        return False

    # Get old driver ID mapping (old id -> old driver_id)
    old_drivers = {r['driver_id']: r['id'] for r in db.execute('SELECT driver_id, id FROM drivers').fetchall()}
    id_mapping = {}
    new_id = 1

    for driver in drivers:
        old_id = old_drivers.get(driver['driver_id'])
        if old_id:
            id_mapping[old_id] = new_id
        driver['new_id'] = new_id
        new_id += 1

    # Update predictions to use new driver IDs before deleting old drivers
    if id_mapping:
        p1_cases = " ".join(f"WHEN {old_id} THEN {new_id}" for old_id, new_id in id_mapping.items())
        p2_cases = p1_cases
        p3_cases = p1_cases
        db.execute(f'''
            UPDATE predictions
            SET p1_driver_id = CASE p1_driver_id {p1_cases} ELSE p1_driver_id END,
                p2_driver_id = CASE p2_driver_id {p2_cases} ELSE p2_driver_id END,
                p3_driver_id = CASE p3_driver_id {p3_cases} ELSE p3_driver_id END
        ''')

    db.execute('DELETE FROM drivers')

    for driver in drivers:
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, team, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (driver['new_id'], driver['driver_id'], driver['name'],
              driver['team'], driver['number'], driver['code'], driver['nationality']))

    db.execute('''
        INSERT INTO metadata (key, value, updated_at)
        VALUES ('drivers_last_refresh', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    ''', (datetime.now().isoformat(),))

    db.commit()
    logger.info(f"Refreshed {len(drivers)} drivers successfully")
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Refresh F1 drivers from OpenF1')
    parser.add_argument('--dry-run', action='store_true', help='Test API connectivity without updating DB')
    args = parser.parse_args()

    if args.dry_run:
        logger.info("Dry run mode - testing API connectivity")
        drivers = fetch_drivers_from_api()
        if drivers:
            logger.info(f"API OK: {len(drivers)} drivers available")
            return 0
        logger.error("API check failed")
        return 1

    logger.info(f"Starting driver refresh (season {F1_SEASON})")

    if not os.path.exists(DATABASE_PATH):
        logger.error(f"Database not found at {DATABASE_PATH}")
        return 1

    db = get_db()
    try:
        success = refresh_drivers(db)
        if success:
            logger.info("Driver refresh completed successfully")
            return 0
        logger.error("Driver refresh failed")
        return 1
    finally:
        db.close()


if __name__ == '__main__':
    sys.exit(main())
