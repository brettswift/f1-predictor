"""Unit tests for scripts/fast_forward.py (BUD-135)."""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

import pytest

# Set environment before importing project modules.
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['F1_SEASON'] = '2026'
os.environ['OPENF1_OFFLINE'] = 'true'

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, 'scripts'))
sys.path.insert(0, os.path.join(_root, 'cron'))
sys.path.insert(0, os.path.join(_root, 'src'))

import fast_forward as ff


def _in_memory_db():
    """Create a DB with the production-ish schema used by fast_forward."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE races (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            round INTEGER NOT NULL,
            date TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            session_key INTEGER
        );
        CREATE TABLE drivers (
            id INTEGER PRIMARY KEY,
            driver_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            number INTEGER NOT NULL,
            code TEXT,
            nationality TEXT
        );
        CREATE TABLE users (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            UNIQUE(user_id, race_id)
        );
        CREATE TABLE results (
            race_id INTEGER PRIMARY KEY,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            had_safety_car INTEGER,
            safety_car_count INTEGER,
            had_virtual_safety_car INTEGER,
            virtual_safety_car_count INTEGER,
            data_source TEXT,
            recorded_at TIMESTAMP
        );
        CREATE TABLE scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            points INTEGER NOT NULL,
            UNIQUE(user_id, race_id)
        );
        CREATE TABLE race_stages (
            race_id INTEGER PRIMARY KEY,
            stage TEXT NOT NULL,
            entered_at TEXT NOT NULL,
            last_poll_at TEXT,
            poll_count INTEGER DEFAULT 0
        );
    ''')
    return conn


def _seed_race_and_drivers(db):
    db.execute('''
        INSERT INTO races (id, name, round, date, status, session_key)
        VALUES (1, 'Test Grand Prix', 10, '2026-10-01 14:00:00', 'open', 9010)
    ''')
    db.executemany(
        'INSERT INTO drivers (id, driver_id, name, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?)',
        [
            (1, 'max_verstappen', 'Max Verstappen', 1, 'VER', 'NED'),
            (2, 'lando_norris', 'Lando Norris', 4, 'NOR', 'GBR'),
            (3, 'charles_leclerc', 'Charles Leclerc', 16, 'LEC', 'MON'),
        ],
    )
    db.commit()


class TestFastForwardLifecycle:
    """BUD-135: phase transitions and scoring output."""

    def test_fast_forward_transitions_through_phases_and_scores(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', ('u1', 'alice'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('u1', 1, 1, 2, 3))
        db.commit()

        ff.fast_forward(
            db, 1,
            results_spec={'p1': 1, 'p2': 4, 'p3': 16},  # 4 -> Norris, 16 -> Leclerc
            phase_delays={p: 0 for p in ff.PHASE_ORDER},
            skip_wait=True,
        )

        race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'completed'

        stage = db.execute('SELECT * FROM race_stages WHERE race_id = 1').fetchone()
        assert stage['stage'] == 'completed'

        result = db.execute('SELECT * FROM results WHERE race_id = 1').fetchone()
        assert result is not None
        assert result['p1_driver_id'] == 1
        assert result['p2_driver_id'] == 2
        assert result['p3_driver_id'] == 3
        assert result['data_source'] == 'mock'

        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('u1', 1)).fetchone()
        assert score is not None
        assert score['points'] == 20

    def test_fast_forward_by_driver_numbers(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', ('u1', 'alice'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('u1', 1, 1, 2, 3))
        db.commit()

        ff.fast_forward(
            db, 1,
            results_spec={'p1': 1, 'p2': 4, 'p3': 16},
            phase_delays={p: 0 for p in ff.PHASE_ORDER},
            skip_wait=True,
        )

        result = db.execute('SELECT * FROM results WHERE race_id = 1').fetchone()
        assert result['p2_driver_id'] == 2  # car number 4 = Lando Norris
        assert result['p3_driver_id'] == 3  # car number 16 = Charles Leclerc

    def test_fast_forward_partial_prediction_scoring(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', ('u1', 'alice'))
        # Predict P1=Verstappen, P2=Norris, P3=Leclerc
        # Results: P1=Verstappen(+10), P2=Leclerc(+0, +1 wrong pos), P3=Norris(+0, +1 wrong pos)
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('u1', 1, 1, 2, 3))
        db.commit()

        ff.fast_forward(
            db, 1,
            results_spec={'p1': 1, 'p2': 16, 'p3': 4},
            phase_delays={p: 0 for p in ff.PHASE_ORDER},
            skip_wait=True,
        )

        score = db.execute('SELECT points FROM scores WHERE user_id = ? AND race_id = ?', ('u1', 1)).fetchone()
        assert score['points'] == 12

    def test_reset_restores_original_state(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        original_date = '2026-10-01 14:00:00'

        ff.fast_forward(
            db, 1,
            results_spec={'p1': 1, 'p2': 4, 'p3': 16},
            phase_delays={p: 0 for p in ff.PHASE_ORDER},
            skip_wait=True,
        )

        ok = ff._reset_race(db, 1)
        assert ok is True

        race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'open'
        assert race['date'] == original_date
        assert race['session_key'] == 9010

        assert db.execute('SELECT 1 FROM results WHERE race_id = 1').fetchone() is None
        assert db.execute('SELECT 1 FROM scores WHERE race_id = 1').fetchone() is None
        assert db.execute('SELECT 1 FROM race_stages WHERE race_id = 1').fetchone() is None
        assert db.execute('SELECT 1 FROM fast_forward_snapshots WHERE race_id = 1').fetchone() is None

    def test_reset_without_snapshot_returns_false(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        ok = ff._reset_race(db, 1)
        assert ok is False

    def test_completed_race_raises(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute("UPDATE races SET status = 'completed' WHERE id = 1")
        db.commit()

        with pytest.raises(ValueError, match='already completed'):
            ff.fast_forward(db, 1, {'p1': 1, 'p2': 4, 'p3': 16},
                            {p: 0 for p in ff.PHASE_ORDER}, skip_wait=True)


class TestFastForwardCLI:
    """CLI entry-point behaviour."""

    def test_main_reset_integration(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', ('u1', 'alice'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('u1', 1, 1, 2, 3))
        db.commit()

        # Use a temp file because main() opens the DB by path.
        import tempfile
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        with sqlite3.connect(path) as src:
            db.backup(src)

        rc = ff.main([
            '--race-id', '1',
            '--results', '{"p1":1,"p2":4,"p3":16}',
            '--skip-wait',
            '--database', path,
        ])
        assert rc == 0

        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        race = conn.execute('SELECT status FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'completed'

        rc = ff.main(['--race-id', '1', '--reset', '--database', path])
        assert rc == 0
        race = conn.execute('SELECT status, date FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'open'
        assert race['date'] == '2026-10-01 14:00:00'
        conn.close()
        os.unlink(path)

    def test_main_requires_results_for_final_phase(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        import tempfile
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        with sqlite3.connect(path) as src:
            db.backup(src)

        rc = ff.main(['--race-id', '1', '--skip-wait', '--database', path])
        assert rc == 1
        os.unlink(path)


class TestPhaseConfiguration:
    """Configurable phase delays."""

    def test_parse_phase_delays_defaults(self):
        class Args:
            phase_delays = None
        delays = ff._parse_phase_delays(Args())
        assert delays == {p: ff.DEFAULT_PHASE_DELAY for p in ff.PHASE_ORDER}

    def test_parse_phase_delays_override(self):
        class Args:
            phase_delays = '{"open": 5, "locked": 10}'
        delays = ff._parse_phase_delays(Args())
        assert delays['open'] == 5
        assert delays['locked'] == 10
        for phase in ff.PHASE_ORDER:
            if phase not in ('open', 'locked'):
                assert delays[phase] == ff.DEFAULT_PHASE_DELAY
