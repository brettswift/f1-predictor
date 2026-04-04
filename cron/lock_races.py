#!/usr/bin/env python3
"""
F1 Race Locker — standalone CronJob to lock races that have started.

Runs every minute as a Kubernetes CronJob. Locks races where:
  - status = 'open' (not yet locked)
  - date < now (race has started)

Uses direct DB update — no HTTP call, no auth needed, no race condition.

Logs "Locked race: {name} (was open since {date})" for each locked race.
"""

import argparse
import os
import sys
import sqlite3
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def lock_races(db):
    """Lock races that have started (date < now) and are still open.

    Returns the number of races locked.
    """
    # Find races that should be locked
    rows = db.execute('''
        SELECT id, name, date
        FROM races
        WHERE status = 'open'
        AND datetime(date) < datetime('now', 'utc')
    ''').fetchall()

    if not rows:
        logger.info("No races to lock")
        return 0

    locked_count = 0
    for row in rows:
        db.execute(
            "UPDATE races SET status = 'locked' WHERE id = ? AND status = 'open'",
            (row['id'],)
        )
        logger.info("Locked race: %s (was open since %s)", row['name'], row['date'])
        locked_count += 1

    db.commit()
    logger.info("Locked %d race(s)", locked_count)
    return locked_count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Lock F1 races that have started')
    args = parser.parse_args()

    logger.info("Starting race locker")

    if not os.path.exists(DATABASE_PATH):
        logger.error("Database not found at %s", DATABASE_PATH)
        return 1

    db = get_db()
    try:
        count = lock_races(db)
        logger.info("Race locker completed: %d races locked", count)
        return 0
    finally:
        db.close()


if __name__ == '__main__':
    sys.exit(main())
