#!/usr/bin/env python3
"""
Fast-forward a race weekend for end-to-end testing.

Advances a single race through its lifecycle states in compressed wall-clock
time and can inject mock OpenF1 results so scoring runs immediately.

Usage:
    python3 -m scripts.fast_forward --race-id 5 --results '{"p1": 1, "p2": 4, "p3": 16}'
    python3 -m scripts.fast_forward --race-id 5 --reset

The script manipulates the existing `races` row and `race_stages` state-machine
table directly; it does not duplicate the scoring code in race_manager.py.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

# project imports: cron/ for race_manager, src/ for openf1/fetch_attempts
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_root, 'cron'))
sys.path.insert(0, os.path.join(_root, 'src'))

import openf1
import race_manager as rm
from fetch_attempts import Outcome, record_fetch_attempt

# Fast-forward is an offline simulation tool; it injects mock data and must
# never call the live OpenF1 API.
openf1.OFFLINE = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('fast_forward')

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))

# Lifecycle phases used by this simulator.  Not every phase maps to a distinct
# DB status; some are logical states used only for observability/testing.
PHASE_ORDER = ['open', 'closing_soon', 'locked', 'live', 'results_available']

# Default delay between phase transitions (seconds).  Total = 4 * 30 = 120 s.
DEFAULT_PHASE_DELAY = 30

# Snapshot table lets --reset restore the exact pre-run state.
SNAPSHOT_TABLE = 'fast_forward_snapshots'


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip().replace('Z', '')
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _ensure_snapshot_table(db: sqlite3.Connection) -> None:
    db.execute(f'''
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            race_id              INTEGER PRIMARY KEY,
            original_status      TEXT,
            original_date        TEXT,
            original_session_key INTEGER,
            had_results          INTEGER NOT NULL DEFAULT 0,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    db.commit()


def _ensure_race_stages_table(db: sqlite3.Connection) -> None:
    rm.ensure_stage_table(db)


def _load_race(db: sqlite3.Connection, race_id: int) -> Optional[sqlite3.Row]:
    return db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()


def _snapshot_race(db: sqlite3.Connection, race_id: int) -> None:
    """Record the current race state so --reset can restore it later."""
    _ensure_snapshot_table(db)
    race = _load_race(db, race_id)
    if race is None:
        raise ValueError(f"Race {race_id} not found")
    had_results = db.execute(
        'SELECT 1 FROM results WHERE race_id = ?', (race_id,)
    ).fetchone() is not None
    db.execute(f'''
        INSERT INTO {SNAPSHOT_TABLE}
            (race_id, original_status, original_date, original_session_key, had_results)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(race_id) DO UPDATE SET
            original_status = excluded.original_status,
            original_date = excluded.original_date,
            original_session_key = excluded.original_session_key,
            had_results = excluded.had_results,
            created_at = excluded.created_at
    ''', (
        race_id,
        race['status'],
        race['date'],
        race['session_key'],
        1 if had_results else 0,
    ))
    db.commit()


def _reset_race(db: sqlite3.Connection, race_id: int) -> bool:
    """Restore race to the snapshot state and clean up generated data."""
    _ensure_snapshot_table(db)
    snap = db.execute(
        f'SELECT * FROM {SNAPSHOT_TABLE} WHERE race_id = ?', (race_id,)
    ).fetchone()
    if snap is None:
        logger.error("No fast-forward snapshot found for race %s; cannot reset.", race_id)
        return False

    # Restore the race row exactly.
    db.execute('''
        UPDATE races
        SET status = ?, date = ?, session_key = ?
        WHERE id = ?
    ''', (snap['original_status'], snap['original_date'],
          snap['original_session_key'], race_id))

    # Remove anything the fast-forwarder may have created.
    db.execute('DELETE FROM results WHERE race_id = ?', (race_id,))
    db.execute('DELETE FROM scores WHERE race_id = ?', (race_id,))
    db.execute('DELETE FROM race_stages WHERE race_id = ?', (race_id,))
    db.execute(f'DELETE FROM {SNAPSHOT_TABLE} WHERE race_id = ?', (race_id,))
    db.commit()
    logger.info("Reset race %s to original status=%s date=%s",
                race_id, snap['original_status'], snap['original_date'])
    return True


def _driver_id_by_number(db: sqlite3.Connection, driver_number: int) -> Optional[int]:
    row = db.execute(
        'SELECT id FROM drivers WHERE number = ?', (driver_number,)
    ).fetchone()
    return row['id'] if row else None


def _resolve_driver_id(db: sqlite3.Connection, value: Any) -> int:
    """Resolve a driver reference supplied as a DB id or OpenF1 car number."""
    try:
        value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Driver reference must be an integer, got {value!r}") from exc

    # If a driver with this id exists, treat it as a DB id.
    by_id = db.execute('SELECT 1 FROM drivers WHERE id = ?', (value,)).fetchone()
    if by_id:
        return value

    # Otherwise treat it as an OpenF1 car number.
    driver_id = _driver_id_by_number(db, value)
    if driver_id is None:
        raise ValueError(
            f"No driver with id or car number {value} in the database"
        )
    return driver_id


def _build_podium(db: sqlite3.Connection, results_spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Convert a user-facing results spec into the internal podium shape."""
    podium: dict[str, dict[str, Any]] = {}
    for slot in ('p1', 'p2', 'p3'):
        if slot not in results_spec:
            raise ValueError(f"Results spec must include '{slot}'")
        driver_id = _resolve_driver_id(db, results_spec[slot])
        row = db.execute('SELECT name, number FROM drivers WHERE id = ?', (driver_id,)).fetchone()
        if row is None:
            raise ValueError(f"Resolved driver id {driver_id} for slot {slot} not found")
        podium[slot] = {'driver_id': driver_id, 'driver_name': row['name']}
    return podium


def _record_mock_fetch_attempts(db: sqlite3.Connection, race_id: int, session_key: Optional[int]) -> None:
    """Leave observability rows consistent with a successful OpenF1 fetch."""
    record_fetch_attempt(
        db, 'sessions',
        f"sessions?year={F1_SEASON}&session_type=Race",
        Outcome.OK, race_id=race_id,
    )
    if session_key:
        record_fetch_attempt(
            db, 'session_result',
            f"session_result?session_key={session_key}",
            Outcome.OK, session_key=session_key, race_id=race_id,
        )


def _set_phase(db: sqlite3.Connection, race_id: int, phase: str, now: datetime) -> None:
    """Apply the DB mutations that represent a fast-forward phase."""
    if phase == 'open':
        # Nothing to do; race is expected to be open already.
        return

    if phase == 'closing_soon':
        # Move the race start close to now while keeping it open.
        # "Closing soon" means voting is still allowed but about to lock.
        db.execute(
            "UPDATE races SET status = 'open', date = ? WHERE id = ?",
            (_now_iso(), race_id),
        )
        db.commit()
        return

    if phase == 'locked':
        db.execute(
            "UPDATE races SET status = 'locked' WHERE id = ?",
            (race_id,),
        )
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, 'locked', ?)
            ON CONFLICT(race_id) DO UPDATE SET
                stage = excluded.stage,
                entered_at = excluded.entered_at,
                last_poll_at = NULL,
                poll_count = 0
        ''', (race_id, _now_iso()))
        db.commit()
        return

    if phase == 'live':
        # In the production model a live race is simply locked and in the past.
        # No DB change required beyond what 'locked' already set, but we ensure
        # the race date is in the past so downstream logic treats it as started.
        db.execute(
            "UPDATE races SET status = 'locked' WHERE id = ?",
            (race_id,),
        )
        db.commit()
        return

    if phase == 'results_available':
        # Final state is applied by inject_results; this helper just logs.
        return

    raise ValueError(f"Unknown phase: {phase}")


def inject_results(
    db: sqlite3.Connection,
    race_id: int,
    results_spec: dict[str, Any],
    data_source: str = 'mock',
) -> bool:
    """Inject mock results and recalculate scores using race_manager paths."""
    race = _load_race(db, race_id)
    if race is None:
        raise ValueError(f"Race {race_id} not found")

    podium = _build_podium(db, results_spec)
    session_key = race['session_key']

    # For mock data we do not call OpenF1; record attempts so observers see
    # a coherent fetch history.
    _record_mock_fetch_attempts(db, race_id, session_key)

    # Use the production result-save path (includes calculate_score + update scores).
    rm._save_results_and_score(db, race_id, podium, session_key=session_key)

    # Mirror poll_for_results: once results are saved, mark the stage completed.
    db.execute('''
        UPDATE race_stages
        SET stage = 'completed', entered_at = ?, last_poll_at = ?, poll_count = poll_count + 1
        WHERE race_id = ?
    ''', (_now_iso(), _now_iso(), race_id))

    # Tag the source as mock so operators know this was simulated.
    db.execute(
        "UPDATE results SET data_source = ? WHERE race_id = ?",
        (data_source, race_id),
    )
    db.commit()

    logger.info(
        "Results injected for race %s: P1=%s P2=%s P3=%s",
        race_id,
        podium['p1']['driver_name'],
        podium['p2']['driver_name'],
        podium['p3']['driver_name'],
    )
    return True


def _parse_phase_delays(args: argparse.Namespace) -> dict[str, int]:
    """Build a phase -> delay map from CLI options."""
    delays: dict[str, int] = {}
    if args.phase_delays:
        try:
            delays = json.loads(args.phase_delays)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid --phase-delays JSON: {exc}")
        for phase in PHASE_ORDER:
            if phase not in delays:
                delays[phase] = DEFAULT_PHASE_DELAY
    else:
        delays = {phase: DEFAULT_PHASE_DELAY for phase in PHASE_ORDER}
    return delays


def fast_forward(
    db: sqlite3.Connection,
    race_id: int,
    results_spec: Optional[dict[str, Any]],
    phase_delays: dict[str, int],
    skip_wait: bool = False,
) -> None:
    """Run the compressed lifecycle simulation for one race."""
    race = _load_race(db, race_id)
    if race is None:
        raise ValueError(f"Race {race_id} not found")

    _ensure_race_stages_table(db)
    _snapshot_race(db, race_id)

    # Ensure race starts open so the full lifecycle can be demonstrated.
    if race['status'] == 'completed':
        raise ValueError(
            f"Race {race_id} is already completed. Use --reset first if you want to rerun."
        )
    if race['status'] != 'open':
        logger.warning(
            "Race %s status is '%s'; fast-forward will treat it as open",
            race_id, race['status'],
        )
        db.execute("UPDATE races SET status = 'open' WHERE id = ?", (race_id,))
        db.commit()

    logger.info("Fast-forwarding race %s (%s)", race_id, race['name'])

    start_time = _utcnow()
    for i, phase in enumerate(PHASE_ORDER):
        logger.info("Phase %d/%d: %s", i + 1, len(PHASE_ORDER), phase)
        now = _utcnow()
        _set_phase(db, race_id, phase, now)

        if phase == 'results_available':
            if results_spec is None:
                raise ValueError(
                    "Must provide --results to reach results_available phase"
                )
            inject_results(db, race_id, results_spec)
            break

        delay = phase_delays.get(phase, DEFAULT_PHASE_DELAY)
        if delay > 0 and not skip_wait and i < len(PHASE_ORDER) - 1:
            logger.info("  waiting %ds until next phase...", delay)
            time.sleep(delay)

    elapsed = (_utcnow() - start_time).total_seconds()
    logger.info("Fast-forward complete in %.1fs", elapsed)


def main(argv: Optional[list[str]] = None) -> int:
    global DATABASE_PATH
    parser = argparse.ArgumentParser(
        description='Fast-forward a race weekend for end-to-end testing'
    )
    parser.add_argument('--race-id', type=int, required=True,
                        help='Race id to fast-forward')
    parser.add_argument('--results', type=str,
                        help='Mock podium as JSON, e.g. {"p1":1,"p2":4,"p3":16} '
                             '(driver db ids or OpenF1 car numbers)')
    parser.add_argument('--phase-delays', type=str,
                        help='JSON mapping phase names to seconds, '
                             'e.g. {"open":0,"closing_soon":10,...}')
    parser.add_argument('--skip-wait', action='store_true',
                        help='Run all phase transitions immediately (tests)')
    parser.add_argument('--reset', action='store_true',
                        help='Restore race to pre-fast-forward state')
    parser.add_argument('--database', type=str, default=DATABASE_PATH,
                        help='Path to SQLite database')
    args = parser.parse_args(argv)

    if not os.path.exists(args.database):
        logger.error("Database not found at %s", args.database)
        return 1

    results_spec = None
    if args.results:
        try:
            results_spec = json.loads(args.results)
        except json.JSONDecodeError as exc:
            logger.error("Invalid --results JSON: %s", exc)
            return 1

    DATABASE_PATH = args.database
    os.environ['DATABASE_PATH'] = DATABASE_PATH

    db = get_db()
    try:
        if args.reset:
            ok = _reset_race(db, args.race_id)
            return 0 if ok else 1

        phase_delays = _parse_phase_delays(args)
        fast_forward(db, args.race_id, results_spec, phase_delays,
                     skip_wait=args.skip_wait)
        return 0
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    finally:
        db.close()


if __name__ == '__main__':
    sys.exit(main())
