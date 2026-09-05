"""Integration test for scripts/fast_forward.py (BUD-135).

Spins up f1-mock-api with its own temp DB, feeds a matching set of races and
drivers into both the predictor's and mock's databases, then runs the full
unpatched fast-forward loop — no monkeypatching of _fetch_podium or any other
ingestion path.
"""

import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

import pytest

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, 'scripts'))
sys.path.insert(0, os.path.join(_root, 'cron'))
sys.path.insert(0, os.path.join(_root, 'src'))

import app as app_module
import fast_forward as ff


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def _wait_for_server(url: str, timeout: float = 15.0) -> bool:
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=2):
                return True
        except Exception:
            time.sleep(0.2)
    return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _setup_env(mock_api: str, predictor_db: str):
    """Set env vars and reload module-level constants."""
    mock_api_v1 = mock_api + '/v1'
    os.environ['OPENF1_API_URL'] = mock_api_v1
    os.environ['DATABASE_PATH'] = predictor_db
    os.environ['F1_SEASON'] = '2026'
    os.environ['OPENF1_OFFLINE'] = 'false'
    app_module.app.config['DATABASE'] = predictor_db
    app_module.app.config['F1_SEASON'] = 2026
    # Patch openf1 module-level vars
    import openf1 as _of
    _of.F1_SEASON = 2026
    _of.OPENF1_BASE_URL = mock_api_v1
    _of.OFFLINE = False
    import race_manager as _rm
    _rm.F1_SEASON = 2026


@pytest.fixture
def predictor_db_path() -> str:
    """Create and seed the predictor's temp DB."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    old_db = os.environ.get('DATABASE_PATH')
    os.environ['DATABASE_PATH'] = path
    app_module.app.config['DATABASE'] = path

    with app_module.app.app_context():
        app_module.init_db()
        db = app_module.get_db()

        db.execute(
            "INSERT INTO races (id, name, round, date, status, session_key) "
            "VALUES (1, 'Integration GP', 1, '2100-01-01 14:00:00', 'open', 1)"
        )
        db.executemany(
            "INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, 'ver', 'Max Verstappen', 'Red Bull Racing', 33, 'VER', 'Dutch'),
                (2, 'nor', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'),
                (3, 'lec', 'Charles Leclerc', 'Ferrari', 16, 'LEC', 'Monegasque'),
            ],
        )
        db.execute("INSERT INTO users (session_id, username) VALUES ('u_int', 'int_tester')")
        db.execute(
            "INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) "
            "VALUES ('u_int', 1, 1, 2, 3)"
        )
        db.commit()

    yield path

    if old_db is None:
        os.environ.pop('DATABASE_PATH', None)
    else:
        os.environ['DATABASE_PATH'] = old_db
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_db_path() -> str:
    """Create and seed f1-mock-api's temp DB."""
    path = tempfile.mktemp(suffix='.db')

    conn = sqlite3.connect(path)
    cur = conn.cursor()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS seasons (
            season TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            round TEXT NOT NULL,
            race_name TEXT,
            circuit_id TEXT,
            circuit_name TEXT,
            circuit_url TEXT,
            locality TEXT,
            country TEXT,
            lat TEXT,
            long TEXT,
            race_url TEXT,
            date TEXT,
            time TEXT,
            start_override TEXT,
            has_results INTEGER DEFAULT 0,
            p1_driver_id TEXT,
            p2_driver_id TEXT,
            p3_driver_id TEXT,
            raw_json TEXT,
            UNIQUE(season, round)
        );
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            permanent_number TEXT,
            code TEXT,
            url TEXT,
            given_name TEXT,
            family_name TEXT,
            date_of_birth TEXT,
            nationality TEXT,
            raw_json TEXT,
            UNIQUE(season, driver_id)
        );
    """)

    cur.execute("INSERT OR IGNORE INTO seasons (season) VALUES ('2026')")
    cur.execute(
        "INSERT INTO races (id, race_name, round, season, date, time, "
        "circuit_id, circuit_name, country, locality) "
        "VALUES (1, 'Integration GP', 1, '2026', '2100-01-01', '14:00:00', "
        "'test_circuit', 'Test Circuit', 'Country', 'City')"
    )
    # driver_id = predictor's driver id as string, so MockAdmin.set_podium
    # sending '1' matches the mock's driver lookup.
    cur.executemany(
        "INSERT INTO drivers (season, driver_id, given_name, family_name, code, "
        "nationality, permanent_number) "
        "VALUES ('2026', ?, ?, ?, ?, ?, ?)",
        [
            ('1', 'Max', 'Verstappen', 'VER', 'Dutch', '33'),
            ('2', 'Lando', 'Norris', 'NOR', 'British', '4'),
            ('3', 'Charles', 'Leclerc', 'LEC', 'Monegasque', '16'),
        ],
    )
    conn.commit()
    conn.close()

    yield path

    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def mock_api(mock_db_path) -> str:
    """Start f1-mock-api with its own temp DB, return base URL."""
    port = _free_port()
    url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env['DATABASE_PATH'] = mock_db_path
    env['FLASK_APP'] = 'src/app.py'
    env['FLASK_DEBUG'] = '0'
    env['ERGAST_BASE'] = 'http://127.0.0.1:9999/'
    env['SECRET_KEY'] = 'test-key'
    env['DEFAULT_SEASON'] = '2026'

    mock_dir = os.path.join(_root, 'f1-mock-api')
    proc = subprocess.Popen(
        [sys.executable, '-m', 'flask', 'run', '--port', str(port), '--no-reload'],
        cwd=mock_dir, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    if not _wait_for_server(url, timeout=15):
        proc.kill()
        proc.wait()
        raise RuntimeError(f"f1-mock-api didn't start on {url}")

    yield url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFastForwardIntegration:
    def test_end_to_end_real_ingestion(self, mock_api, predictor_db_path):
        """Full unpatched loop — exercises real OpenF1 fetch through mock."""
        start_wall = datetime.now(timezone.utc)
        _setup_env(mock_api, predictor_db_path)

        with app_module.app.app_context():
            db = app_module.get_db()
            ff.fast_forward(
                db, race_id=1,
                results_spec={'p1': 1, 'p2': 2, 'p3': 3},
                mock=ff.MockAdmin(mock_api), mock_race_id=1,
                step_minutes=95,
                frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
            )

            race = db.execute('SELECT * FROM races WHERE id = 1').fetchone()
            assert race['status'] == 'completed', f"got {race['status']}"

            results = db.execute('SELECT * FROM results WHERE race_id = 1').fetchone()
            assert results is not None, "no results row"

            scores = db.execute(
                'SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
                ('u_int', 1),
            ).fetchone()
            assert scores is not None, "no scores row"
            assert scores['points'] == 20, f"got {scores['points']}"

        elapsed = (datetime.now(timezone.utc) - start_wall).total_seconds()
        assert elapsed < 300, f'took {elapsed}s'

    def test_reset_then_rerun(self, mock_api, predictor_db_path):
        """Reset restores open state; second run re-completes."""
        _setup_env(mock_api, predictor_db_path)

        with app_module.app.app_context():
            db = app_module.get_db()

            ff.fast_forward(
                db, race_id=1,
                results_spec={'p1': 2, 'p2': 1, 'p3': 3},
                mock=ff.MockAdmin(mock_api), mock_race_id=1,
                step_minutes=95,
                frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
            )

            ok = ff._reset_race(db, 1)
            assert ok is True
            race = db.execute('SELECT status FROM races WHERE id = 1').fetchone()
            assert race['status'] == 'open'

            ff.fast_forward(
                db, race_id=1,
                results_spec={'p1': 2, 'p2': 1, 'p3': 3},
                mock=ff.MockAdmin(mock_api), mock_race_id=1,
                step_minutes=95,
                frozen_at=datetime(2100, 1, 1, 12, 0, tzinfo=timezone.utc),
            )
            race2 = db.execute('SELECT status FROM races WHERE id = 1').fetchone()
            assert race2['status'] == 'completed'