#!/usr/bin/env python3
"""
F1 Driver Refresh CronJob - Refreshes driver data from Ergast API weekly.
Runs as a Kubernetes CronJob in the f1-predictor namespace.
"""

import argparse
import os
import sys
import sqlite3
import requests
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_API_BASE = os.environ.get('F1_API_URL', 'https://api.jolpi.ca/ergast/f1').rstrip('/')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_drivers_from_api():
    """Fetch drivers from Ergast F1 API."""
    url = f"{F1_API_BASE}/{F1_SEASON}/drivers.json"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        drivers = []
        driver_list = data.get('MRData', {}).get('DriverTable', {}).get('Drivers', [])

        for idx, driver in enumerate(driver_list, start=1):
            drivers.append({
                'id': idx,
                'driver_id': driver.get('driverId'),
                'name': f"{driver.get('givenName', '')} {driver.get('familyName', '')}".strip(),
                'number': int(driver.get('permanentNumber', 0)) if driver.get('permanentNumber') else 0,
                'code': driver.get('code'),
                'nationality': driver.get('nationality'),
                'team': None
            })

        logger.info(f"Fetched {len(drivers)} drivers from API")
        return drivers

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Unexpected API response format: {e}")
        return None


def refresh_drivers(db):
    """Refresh drivers in the database from API data.

    Preserves predictions by remapping driver IDs when possible.
    """
    logger.info("Starting driver refresh...")

    drivers = fetch_drivers_from_api()
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
    parser = argparse.ArgumentParser(description='Refresh F1 drivers from API')
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
