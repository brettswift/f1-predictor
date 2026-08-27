#!/usr/bin/env python3
"""
Fast-forward a full race weekend for end-to-end testing (BUD-135).

Drives one race end-to-end (open → locked → results ingested → scored) in
well under 5 minutes by:

  * Advancing virtual time with tests/utils/time_control.TimeController
    (patches app._now_utc / race_manager._utcnow; no time.sleep calls).
  * Transitioning results through the f1-mock-api's *real admin endpoints*:
      POST /admin/race/<id>/start
      POST /admin/race/<id>/podium
      POST /admin/race/<id>/finish
      POST /admin/reseed
  * Triggering the predictor's existing scoring path
    (cron/race_manager.poll_for_results) so scores/results/leaderboard
    update exactly as they do in production.

The predictor is pointed at the mock API through OPENF1_API_URL /
OPENF1_BASE_URL / API_BASE_URL env vars.

Usage:
    python -m scripts.fast_forward --race-id 5 \
        --podium '{"p1": 1, "p2": 4, "p3": 16}' \
        --mock-race-id 12

    python -m scripts.fast_forward --race-id 5 --reset

    python -m scripts.fast_forward --reseed --mock-race-id 12
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Optional

# project imports: cron/ for race_manager, src/ for app hooks.
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_root, 'cron'))
sys.path.insert(0, os.path.join(_root, 'src'))
sys.path.insert(0, _root)

import requests
import race_manager as rm

from tests.utils.time_control import TimeController

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('fast_forward')

DATABASE_PATH = os.environ.get('DATABASE_PATH', '/data/f1_predictions.db')
F1_SEASON = int(os.environ.get('F1_SEASON', '2026'))

# Mock API base URL can come from any of these; explicit CLI wins.
DEFAULT_MOCK_API_URL = (
    os.environ.get('OPENF1_BASE_URL')
    or os.environ.get('OPENF1_API_URL')
    or os.environ.get('API_BASE_URL')
    or ''
)

# Virtual time step used to advance between phases. The predictor's
# ingest window requires the race to have started >= 90 min in the past,
# and race_manager polling transitions require >= 1h30m since lock.
DEFAULT_STEP_MINUTES = 20


# ---------------------------------------------------------------------------
# DB / snapshot helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime] = None) -> str:
    return (now or _utcnow()).strftime('%Y-%m-%dT%H:%M:%SZ')


SNAPSHOT_TABLE = 'fast_forward_snapshots'


def _ensure_snapshot_table(db: sqlite3.Connection) -> None:
    db.execute(f'''
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            race_id INTEGER PRIMARY KEY,
            original_status TEXT,
            original_date TEXT,
            original_session_key INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    db.commit()


def _snapshot_race(db: sqlite3.Connection, race_id: int) -> None:
    _ensure_snapshot_table(db)
    race = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if race is None:
        raise ValueError(f"Race {race_id} not found")
    db.execute(f'''
        INSERT INTO {SNAPSHOT_TABLE} (race_id, original_status, original_date, original_session_key)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(race_id) DO UPDATE SET
            original_status = excluded.original_status,
            original_date = excluded.original_date,
            original_session_key = excluded.original_session_key,
            created_at = excluded.created_at
    ''', (race_id, race['status'], race['date'], race['session_key']))
    db.commit()


def _reset_race(db: sqlite3.Connection, race_id: int) -> bool:
    """Restore race state and drop any generated results/scores/stages."""
    _ensure_snapshot_table(db)
    snap = db.execute(
        f'SELECT * FROM {SNAPSHOT_TABLE} WHERE race_id = ?', (race_id,)
    ).fetchone()
    if snap is None:
        logger.error('No snapshot found for race %s; cannot reset.', race_id)
        return False

    db.execute('''
        UPDATE races SET status = ?, date = ?, session_key = ? WHERE id = ?
    ''', (snap['original_status'], snap['original_date'],
          snap['original_session_key'], race_id))
    db.execute('DELETE FROM results WHERE race_id = ?', (race_id,))
    db.execute('DELETE FROM scores WHERE race_id = ?', (race_id,))
    db.execute('DELETE FROM race_stages WHERE race_id = ?', (race_id,))
    db.execute(f'DELETE FROM {SNAPSHOT_TABLE} WHERE race_id = ?', (race_id,))
    db.commit()
    logger.info('Reset race %s (status=%s)', race_id, snap['original_status'])
    return True


# ---------------------------------------------------------------------------
# Mock API admin calls (all state transitions go through these endpoints)
# ---------------------------------------------------------------------------

class MockAdmin:
    """Thin wrapper over f1-mock-api's admin endpoints."""

    def __init__(self, base_url: str):
        if not base_url:
            raise ValueError('mock api base URL is required')
        self.base = base_url.rstrip('/')

    def _post(self, path: str, data: Optional[dict] = None) -> bool:
        url = f'{self.base}{path}'
        try:
            resp = requests.post(url, data=data or {}, timeout=10)
        except requests.RequestException as exc:
            logger.error('POST %s failed: %s', url, exc)
            return False
        if resp.status_code not in (200, 302):
            logger.error('POST %s -> %s', url, resp.status_code)
            return False
        logger.info('POST %s -> %s', url, resp.status_code)
        return True

    def set_start(self, race_id: int, start_override: str = '') -> bool:
        return self._post(f'/admin/race/{race_id}/start',
                          {'start_override': start_override})

    def set_podium(self, race_id: int, p1: Any, p2: Any, p3: Any) -> bool:
        return self._post(f'/admin/race/{race_id}/podium', {
            'p1_driver_id': str(p1),
            'p2_driver_id': str(p2),
            'p3_driver_id': str(p3),
        })

    def finish(self, race_id: int) -> bool:
        return self._post(f'/admin/race/{race_id}/finish')

    def reseed(self) -> bool:
        return self._post('/admin/reseed')


# ---------------------------------------------------------------------------
# Scoring trigger — uses the predictor's real ingestion path
# ---------------------------------------------------------------------------

def _trigger_ingest(db: sqlite3.Connection) -> None:
    """Run race_manager's state machine once to process one pending race.

    We patch its clock to match the TimeController's frozen value so all
    transitions are deterministic and offline-safe.
    """
    time_now = getattr(rm, '_current_fast_forward_time', _utcnow)()
    rm.promote_to_watching(db, time_now)
    rm.promote_to_locked(db, time_now)
    rm.promote_to_polling(db, time_now)
    rm.poll_for_results(db, time_now)


def _score_via_app(db: sqlite3.Connection) -> int:
    """Run the predictor's own check_and_ingest_results (preferred hook)."""
    import app as app_module  # imported lazily so script can run standalone

    with app_module.app.app_context():
        updated, _ = app_module.check_and_ingest_results(db)
    return len(updated)


# ---------------------------------------------------------------------------
# Core driver
# ---------------------------------------------------------------------------

def _resolve_driver_id(db: sqlite3.Connection, value: Any) -> int:
    """Resolve a driver reference supplied as a DB id or OpenF1 car number."""
    try:
        value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'Driver reference must be an integer, got {value!r}') from exc

    by_id = db.execute('SELECT 1 FROM drivers WHERE id = ?', (value,)).fetchone()
    if by_id:
        return value

    row = db.execute('SELECT id FROM drivers WHERE number = ?', (value,)).fetchone()
    if row:
        return row['id']

    raise ValueError(f'No driver with id or car number {value} in the database')


def _podium_to_mock_ids(db: sqlite3.Connection, spec: dict[str, Any]) -> tuple[int, int, int]:
    """Resolve {p1,p2,p3} into mock-API driver ids (the predictor's DB ids)."""
    return (
        _resolve_driver_id(db, spec['p1']),
        _resolve_driver_id(db, spec['p2']),
        _resolve_driver_id(db, spec['p3']),
    )


def fast_forward(
    db: sqlite3.Connection,
    race_id: int,
    results_spec: Optional[dict[str, Any]],
    mock: Optional[MockAdmin] = None,
    mock_race_id: Optional[int] = None,
    step_minutes: int = DEFAULT_STEP_MINUTES,
    frozen_at: Optional[datetime] = None,
) -> None:
    """
    Run the race end-to-end through the predictor's real ingestion path.

    The TimeController freezes _now_utc so subsequent calls to
    app._now_utc/race_manager._utcnow return the simulated frozen value.
    We advance the clock by `step_minutes` between phases.
    """
    race = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
    if race is None:
        raise ValueError(f'Race {race_id} not found')

    rm.ensure_stage_table(db)

    if race['status'] == 'completed':
        raise ValueError(
            f'Race {race_id} is already completed. Use --reset first if you want to rerun.'
        )
    if race['status'] != 'open':
        logger.warning(
            'Race %s status is %s; fast-forward will assume open',
            race_id, race['status'],
        )
        db.execute("UPDATE races SET status = 'open' WHERE id = ?", (race_id,))
        db.commit()

    _snapshot_race(db, race_id)

    controller = TimeController(target='app._now_utc')
    controller.freeze(frozen_at or _utcnow())

    # Some paths (cron/race_manager) call their own _utcnow; bind a tweakable
    # clock hook that we update each step.
    current_time = controller.frozen_time
    rm._current_fast_forward_time = lambda: current_time  # type: ignore[attr-defined]

    logger.info('Fast-forwarding race %s (%s) starting at %s',
                race_id, race['name'], current_time.isoformat())

    try:
        def _advance() -> None:
            nonlocal current_time
            controller.advance(minutes=step_minutes)
            current_time = controller.frozen_time
            logger.info('advanced to %s', current_time.isoformat())

        # Phase 1: race start (admin/race/<id>/start)
        if mock and mock_race_id is not None:
            mock.set_start(
                mock_race_id,
                start_override=_now_iso(controller.frozen_time),
            )
        _advance()

        # Phase 2: lock in the predictor (auto-lock cron semantics)
        db.execute("UPDATE races SET status = 'locked' WHERE id = ?", (race_id,))
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, 'locked', ?)
            ON CONFLICT(race_id) DO UPDATE SET
                stage = excluded.stage,
                entered_at = excluded.entered_at,
                last_poll_at = NULL,
                poll_count = 0
        ''', (race_id, _now_iso(controller.frozen_time)))
        db.commit()
        _advance()

        # Phase 3: publish podium on the mock
        if results_spec is None:
            raise ValueError('podium results spec is required')
        p1_id, p2_id, p3_id = _podium_to_mock_ids(db, results_spec)
        if mock and mock_race_id is not None:
            mock.set_podium(mock_race_id, p1_id, p2_id, p3_id)
            logger.info('podium set at %s', controller.frozen_time.isoformat())
        _advance()

        # Phase 4: mock marks race finished
        if mock and mock_race_id is not None:
            mock.finish(mock_race_id)
            logger.info('race finished at %s', controller.frozen_time.isoformat())
        _advance()

        # Phase 5: drive the predictor's real ingestion path
        # race_manager.poll_for_results handles fetch_attempts + scoring.
        _trigger_ingest(db)

        if db.execute('SELECT 1 FROM results WHERE race_id = ?', (race_id,)).fetchone() is None:
            logger.warning(
                'poll_for_results did not ingest; falling back to app.check_and_ingest_results'
            )
            _score_via_app(db)

        result_row = db.execute(
            'SELECT p1_driver_id, p2_driver_id, p3_driver_id FROM results WHERE race_id = ?',
            (race_id,),
        ).fetchone()
        if result_row is None:
            raise ValueError(f'results still missing for race {race_id} after ingest')

        scores = db.execute(
            'SELECT COUNT(*) FROM scores WHERE race_id = ?', (race_id,)
        ).fetchone()[0]
        logger.info('race %s completed: P1=%s P2=%s P3=%s scores=%d row(s)',
                    race_id, result_row['p1_driver_id'],
                    result_row['p2_driver_id'], result_row['p3_driver_id'], scores)
    finally:
        controller.unfreeze()
        if hasattr(rm, '_current_fast_forward_time'):
            del rm._current_fast_forward_time  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    global DATABASE_PATH

    parser = argparse.ArgumentParser(
        description='Fast-forward a race weekend for end-to-end testing'
    )
    parser.add_argument('--race-id', type=int,
                        help='Predictor race id to fast-forward')
    parser.add_argument('--mock-race-id', type=int,
                        help='Mock API race id to manipulate (defaults to --race-id)')
    parser.add_argument('--podium', '--results', dest='podium', type=str,
                        help='Mock podium JSON, e.g. {"p1":1,"p2":4,"p3":16}')
    parser.add_argument('--mock-api-url', type=str, default=DEFAULT_MOCK_API_URL,
                        help='f1-mock-api base URL (e.g., http://127.0.0.1:5001)')
    parser.add_argument('--reset', action='store_true',
                        help='Reset race to its pre-fast-forward state')
    parser.add_argument('--reseed', action='store_true',
                        help='Call POST /admin/reseed on the mock API')
    parser.add_argument('--step-minutes', type=int, default=DEFAULT_STEP_MINUTES,
                        help='Minutes to advance per phase (default 20)')
    parser.add_argument('--database', type=str, default=DATABASE_PATH,
                        help='Path to predictor SQLite database')
    args = parser.parse_args(argv)

    if args.reset or args.reseed:
        # reset/reseed still requires DB to be reachable.
        if not os.path.exists(args.database):
            logger.error('Database not found at %s', args.database)
            return 1
        DATABASE_PATH = args.database
        db = get_db()
        try:
            if args.reseed:
                mock = MockAdmin(args.mock_api_url)
                ok = mock.reseed()
                return 0 if ok else 1
            ok = _reset_race(db, args.race_id)
            return 0 if ok else 1
        finally:
            db.close()

    if not args.race_id:
        logger.error('--race-id is required unless --reseed')
        return 1

    if not os.path.exists(args.database):
        logger.error('Database not found at %s', args.database)
        return 1

    results_spec = None
    if args.podium:
        try:
            results_spec = json.loads(args.podium)
        except json.JSONDecodeError as exc:
            logger.error('Invalid --podium JSON: %s', exc)
            return 1

    if results_spec is None:
        logger.error('--podium (or --results) is required')
        return 1

    DATABASE_PATH = args.database
    mock = MockAdmin(args.mock_api_url) if args.mock_api_url else None
    mock_race_id = args.mock_race_id if args.mock_race_id is not None else args.race_id

    db = get_db()
    try:
        fast_forward(
            db,
            args.race_id,
            results_spec,
            mock=mock,
            mock_race_id=mock_race_id,
            step_minutes=args.step_minutes,
        )
        return 0
    except ValueError as exc:
        logger.error('%s', exc)
        return 1
    finally:
        db.close()


if __name__ == '__main__':
    sys.exit(main())
