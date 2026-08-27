"""Integration test for scripts/fast_forward.py (BUD-135).

Simulates a full race weekend in well under 5 minutes using TimeController
and the predictor's real ingestion path.
"""

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, 'scripts'))
sys.path.insert(0, os.path.join(_root, 'cron'))
sys.path.insert(0, os.path.join(_root, 'src'))

import app as app_module
import fast_forward as ff


@pytest.fixture
def predictor_app():
    """Isolate the predictor DB to a temp file for the integration run."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    orig = os.environ.get('DATABASE_PATH')
    os.environ['DATABASE_PATH'] = path
    app_module.app.config['DATABASE'] = path

    with app_module.app.app_context():
        app_module.init_db()
        db = app_module.get_db()

        db.execute('''
            INSERT INTO races (id, name, round, date, status, session_key)
            VALUES (1, 'Integration GP', 1, '2100-01-01 14:00:00', 'open', 42)
        ''')
        db.executemany(
            'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (1, 'ver', 'Max Verstappen', 'RB', 1, 'VER', 'NE'),
                (2, 'nor', 'Lando Norris', 'MC', 4, 'NOR', 'GB'),
                (3, 'lec', 'Charles Leclerc', 'FE', 16, 'LEC', 'MC'),
            ],
        )
        db.execute("INSERT INTO users (session_id, username) VALUES ('u_int', 'int_tester')")
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES ('u_int', 1, 1, 2, 3)
        ''')
        db.commit()
        yield app_module.app

    if orig is None:
        os.environ.pop('DATABASE_PATH', None)
    else:
        os.environ['DATABASE_PATH'] = orig

    if os.path.exists(path):
        os.unlink(path)


class TestFastForwardIntegration:
    def test_end_to_end_under_5min(self, predictor_app):
        start_wall = datetime.now(timezone.utc)
        with app_module.app.app_context():
            db = app_module.get_db()

            # Verstappen P1(10), Norris P2(6), Leclerc P3(4) = 20 points.
            def fake_fetch_podium(db_in, session_key, race_id=None, race_date=None):
                return {
                    'p1': {'driver_id': 1, 'driver_name': 'Max Verstappen'},
                    'p2': {'driver_id': 2, 'driver_name': 'Lando Norris'},
                    'p3': {'driver_id': 3, 'driver_name': 'Charles Leclerc'},
                }

            with patch('race_manager._fetch_podium', side_effect=fake_fetch_podium):
                ff.fast_forward(
                    db, 1,
                    results_spec={'p1': 1, 'p2': 2, 'p3': 3},
                    mock=None,
                    mock_race_id=None,
                    step_minutes=95,
                    frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
                )

            race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
            assert race['status'] == 'completed'

            results = db.execute('SELECT * FROM results WHERE race_id = 1').fetchone()
            assert results is not None

            scores = db.execute(
                'SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
                ('u_int', 1),
            ).fetchone()
            assert scores['points'] == 20

        elapsed = (datetime.now(timezone.utc) - start_wall).total_seconds()
        assert elapsed < 300, f'fast-forward took {elapsed}s (limit 300)'

    def test_reset_then_rerun_is_possible(self, predictor_app):
        with app_module.app.app_context():
            db = app_module.get_db()

            def fake_fetch_podium(db_in, session_key, race_id=None, race_date=None):
                return {
                    'p1': {'driver_id': 2, 'driver_name': 'Lando Norris'},
                    'p2': {'driver_id': 1, 'driver_name': 'Max Verstappen'},
                    'p3': {'driver_id': 3, 'driver_name': 'Charles Leclerc'},
                }

            with patch('race_manager._fetch_podium', side_effect=fake_fetch_podium):
                ff.fast_forward(
                    db, 1,
                    results_spec={'p1': 4, 'p2': 1, 'p3': 16},
                    mock=None,
                    mock_race_id=None,
                    step_minutes=95,
                    frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
                )

            ok = ff._reset_race(db, 1)
            assert ok is True
            race = db.execute('SELECT status FROM races WHERE id = 1').fetchone()
            assert race['status'] == 'open'

            with app_module.app.app_context():
                db2 = app_module.get_db()
                with patch('race_manager._fetch_podium', side_effect=fake_fetch_podium):
                    ff.fast_forward(
                        db2, 1,
                        results_spec={'p1': 4, 'p2': 1, 'p3': 16},
                        mock=None,
                        mock_race_id=None,
                        step_minutes=95,
                        frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
                    )
                race = db2.execute('SELECT status FROM races WHERE id = 1').fetchone()
                assert race['status'] == 'completed'
