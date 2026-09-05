"""Unit tests for scripts/fast_forward.py (BUD-135)."""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import MagicMock, patch

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


class FakeMock:
    """Fake MockAdmin capturing admin endpoint invocations."""

    def __init__(self):
        self.starts: list[dict[str, Any]] = []
        self.podiums: list[dict[str, Any]] = []
        self.finishes: list[int] = []

    def set_start(self, race_id: int, start_override: str = '') -> bool:
        self.starts.append({'race_id': race_id, 'start_override': start_override})
        return True

    def set_podium(self, race_id: int, p1: int, p2: int, p3: int) -> bool:
        self.podiums.append({'race_id': race_id, 'p1': p1, 'p2': p2, 'p3': p3})
        return True

    def finish(self, race_id: int) -> bool:
        self.finishes.append(race_id)
        return True

    def reseed(self) -> bool:
        return True


class TestFastForwardLifecycle:
    """End-to-end lifecycle driven through the real ingestion functions."""

    def _patched_ingest(self, db, p1: int = 1, p2: int = 2, p3: int = 3):
        """Patch race_manager._fetch_podium to return the given podium."""
        def fake_fetch_podium(db_in, session_key, race_id=None, race_date=None):
            return {
                'p1': {'driver_id': p1, 'driver_name': 'P1'},
                'p2': {'driver_id': p2, 'driver_name': 'P2'},
                'p3': {'driver_id': p3, 'driver_name': 'P3'},
            }
        return fake_fetch_podium

    def test_fast_forward_ingests_scores_and_marks_completed(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute("INSERT INTO users (session_id, username) VALUES ('u1', 'alice')")
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES ('u1', 1, 1, 2, 3)
        ''')
        db.commit()

        fake_fetched = self._patched_ingest(db, 1, 2, 3)

        with patch('race_manager._fetch_podium', side_effect=fake_fetched):
            ff.fast_forward(
                db, 1,
                results_spec={'p1': 1, 'p2': 2, 'p3': 3},
                mock=FakeMock(),
                mock_race_id=1,
                step_minutes=95,
                frozen_at=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc),
            )

        race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'completed'

        result = db.execute('SELECT * FROM results WHERE race_id = 1').fetchone()
        assert result is not None
        assert result['p1_driver_id'] == 1
        assert result['p2_driver_id'] == 2
        assert result['p3_driver_id'] == 3

        score = db.execute(
            'SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
            ('u1', 1),
        ).fetchone()
        assert score is not None
        assert score['points'] == 20

    def test_fast_forward_uses_admin_endpoints_when_mock_given(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute("INSERT INTO users (session_id, username) VALUES ('u1', 'alice')")
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES ('u1', 1, 1, 2, 3)
        ''')
        db.commit()

        fake_fetched = self._patched_ingest(db, 1, 2, 3)
        fake_mock = FakeMock()

        with patch('race_manager._fetch_podium', side_effect=fake_fetched):
            ff.fast_forward(
                db, 1,
                results_spec={'p1': 1, 'p2': 2, 'p3': 3},
                mock=fake_mock,
                mock_race_id=7,
                step_minutes=95,
                frozen_at=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc),
            )

        # Sequence: start -> podium -> finish on the *mock race id* (7).
        assert fake_mock.starts and fake_mock.starts[0]['race_id'] == 7
        assert fake_mock.podiums and fake_mock.podiums[0]['race_id'] == 7
        assert fake_mock.finishes and fake_mock.finishes[0] == 7

    def test_reset_restores_state(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        original_date = '2026-10-01 14:00:00'

        fake_fetched = self._patched_ingest(db, 1, 2, 3)

        with patch('race_manager._fetch_podium', side_effect=fake_fetched):
            ff.fast_forward(
                db, 1,
                results_spec={'p1': 1, 'p2': 2, 'p3': 3},
                mock=FakeMock(),
                mock_race_id=1,
                step_minutes=95,
                frozen_at=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc),
            )

        ok = ff._reset_race(db, 1)
        assert ok is True

        race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
        assert race['status'] == 'open'
        assert race['date'] == original_date
        assert db.execute('SELECT 1 FROM results WHERE race_id = 1').fetchone() is None
        assert db.execute('SELECT 1 FROM scores WHERE race_id = 1').fetchone() is None
        assert db.execute('SELECT 1 FROM fast_forward_snapshots WHERE race_id = 1').fetchone() is None

    def test_completed_race_raises(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        db.execute("UPDATE races SET status = 'completed' WHERE id = 1")
        db.commit()

        with pytest.raises(ValueError, match='already completed'):
            ff.fast_forward(db, 1, {'p1': 1, 'p2': 2, 'p3': 3},
                            mock=FakeMock(), mock_race_id=1, step_minutes=95)


class TestDriverResolution:
    def test_resolve_by_db_id(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        assert ff._resolve_driver_id(db, 1) == 1

    def test_resolve_by_car_number(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        assert ff._resolve_driver_id(db, 4) == 2

    def test_resolve_unknown_raises(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        with pytest.raises(ValueError, match='No driver'):
            ff._resolve_driver_id(db, 999)


class TestCLI:
    """CLI-level behaviour (reset/reseed/podium parsing)."""

    def _db_file(self, db):
        import tempfile
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        with sqlite3.connect(path) as src:
            db.backup(src)
        return path

    def test_main_reseed_calls_admin(self):
        path = self._db_file(_in_memory_db())
        fake_mock = FakeMock()
        with patch.object(ff, 'MockAdmin', return_value=fake_mock):
            rc = ff.main(['--reseed', '--database', path,
                          '--mock-api-url', 'http://mock'])
        assert rc == 0
        os.unlink(path)

    def test_main_reset_without_snapshot_returns_false(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        path = self._db_file(db)
        fake_mock = FakeMock()
        with patch.object(ff, 'MockAdmin', return_value=fake_mock):
            rc = ff.main(['--reset', '--race-id', '1', '--database', path])
        assert rc == 1
        os.unlink(path)

    def test_main_requires_podium(self):
        db = _in_memory_db()
        _seed_race_and_drivers(db)
        path = self._db_file(db)
        rc = ff.main(['--race-id', '1', '--database', path])
        assert rc == 1
        os.unlink(path)
