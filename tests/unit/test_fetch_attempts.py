"""Unit tests for fetch-attempt observability (BUD-125 / F1-03).

Covers the persistence half of "record fetch failures and fail the Job":
  * fetch_attempts rows are written with the right outcome/status.
  * a hard failure (exhausted retries, or empty >4h post-race) makes the
    cron entrypoints exit non-zero.
  * a transient failure that succeeds on retry does NOT fail the Job.
  * /health surfaces last-successful-fetch age without needing DB access.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

os.environ['DATABASE_PATH'] = ':memory:'
os.environ['TESTING'] = 'true'
os.environ['F1_SEASON'] = '2026'
os.environ['OPENF1_OFFLINE'] = 'true'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import pytest

import fetch_attempts as fa


def _db():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    return conn


class TestRecordFetchAttempt:
    def test_creates_table_lazily(self):
        db = _db()
        fa.record_fetch_attempt(db, 'session_result', 'session_result?session_key=1', fa.Outcome.OK)
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert 'fetch_attempts' in tables

    def test_records_required_fields(self):
        db = _db()
        fa.record_fetch_attempt(
            db, 'session_result', 'session_result?session_key=42', fa.Outcome.HTTP_ERROR,
            http_status=503, session_key=42, race_id=7, detail='boom',
        )
        row = db.execute('SELECT * FROM fetch_attempts').fetchone()
        assert row['endpoint'] == 'session_result'
        assert row['cache_key'] == 'session_result?session_key=42'
        assert row['outcome'] == 'http_error'
        assert row['http_status'] == 503
        assert row['session_key'] == 42
        assert row['race_id'] == 7
        assert row['attempted_at'] is not None

    def test_rejects_unknown_outcome_via_check_constraint(self):
        db = _db()
        fa.ensure_fetch_attempts_table(db)
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO fetch_attempts (endpoint, cache_key, outcome) VALUES (?, ?, ?)",
                ('session_result', 'k', 'not_a_real_outcome'),
            )


class TestOutcomeForError:
    def test_429_status_is_rate_limited(self):
        class FakeErr(Exception):
            status_code = 429
            is_timeout = False

        assert fa.outcome_for_error(FakeErr()) == fa.Outcome.RATE_LIMITED

    def test_timeout_flag_is_timeout(self):
        class FakeErr(Exception):
            status_code = None
            is_timeout = True

        assert fa.outcome_for_error(FakeErr()) == fa.Outcome.TIMEOUT

    def test_other_status_is_http_error(self):
        class FakeErr(Exception):
            status_code = 500
            is_timeout = False

        assert fa.outcome_for_error(FakeErr()) == fa.Outcome.HTTP_ERROR

    def test_unclassified_exception_is_http_error(self):
        assert fa.outcome_for_error(RuntimeError("boom")) == fa.Outcome.HTTP_ERROR


class TestRaceFinishedLongAgo:
    def test_within_window_is_false(self):
        now = datetime(2026, 6, 15, 17, 0, 0, tzinfo=timezone.utc)
        assert fa.race_finished_long_ago('2026-06-15 14:00:00', now=now) is False

    def test_past_window_is_true(self):
        now = datetime(2026, 6, 15, 20, 0, 0, tzinfo=timezone.utc)
        assert fa.race_finished_long_ago('2026-06-15 14:00:00', now=now) is True

    def test_unparseable_date_is_false(self):
        assert fa.race_finished_long_ago('not-a-date') is False


class TestLastSuccessfulFetchAt:
    def test_none_when_no_ok_rows(self):
        db = _db()
        assert fa.last_successful_fetch_at(db) is None

    def test_returns_latest_ok_timestamp(self):
        db = _db()
        fa.record_fetch_attempt(db, 'session_result', 'k1', fa.Outcome.HTTP_ERROR)
        fa.record_fetch_attempt(db, 'session_result', 'k2', fa.Outcome.OK)
        assert fa.last_successful_fetch_at(db) is not None


class TestFetchRaceResultsMainExitCode:
    """main() in cron/fetch_race_results.py exits non-zero on a hard failure."""

    def _seed(self, db):
        db.execute('''
            CREATE TABLE drivers (id INTEGER PRIMARY KEY, driver_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL, number INTEGER NOT NULL, code TEXT, nationality TEXT)
        ''')
        db.execute('''
            CREATE TABLE races (id INTEGER PRIMARY KEY, name TEXT NOT NULL, round INTEGER NOT NULL,
                date TEXT NOT NULL, status TEXT DEFAULT 'open', session_key INTEGER)
        ''')
        db.execute('''
            CREATE TABLE results (race_id INTEGER PRIMARY KEY, p1_driver_id INTEGER,
                p2_driver_id INTEGER, p3_driver_id INTEGER)
        ''')
        db.commit()

    def test_main_exits_nonzero_on_exhausted_retry_failure(self, tmp_path, monkeypatch):
        import fetch_race_results as fr
        import openf1

        db_path = str(tmp_path / 'f1.db')
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
        self._seed(db)
        long_ago = (datetime.now(timezone.utc) - timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute(
            "INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)",
            (1, 'Test GP', 1, long_ago, 'locked', 9001),
        )
        db.commit()
        db.close()

        monkeypatch.setattr(fr, 'DATABASE_PATH', db_path)
        monkeypatch.setattr(sys, 'argv', ['fetch_race_results.py'])
        err = openf1.OpenF1Error("boom", status_code=500)
        with patch('fetch_race_results.openf1.get_podium', side_effect=err):
            with pytest.raises(SystemExit) as exc_info:
                fr.main()

        assert exc_info.value.code != 0

        check_db = sqlite3.connect(db_path)
        check_db.row_factory = sqlite3.Row
        row = check_db.execute("SELECT outcome FROM fetch_attempts WHERE race_id = 1").fetchone()
        check_db.close()
        assert row is not None
        assert row['outcome'] == 'http_error'

    def test_main_does_not_exit_nonzero_when_all_ok(self, tmp_path, monkeypatch):
        import fetch_race_results as fr

        db_path = str(tmp_path / 'f1.db')
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
        self._seed(db)
        db.commit()
        db.close()

        monkeypatch.setattr(fr, 'DATABASE_PATH', db_path)
        monkeypatch.setattr(sys, 'argv', ['fetch_race_results.py'])

        # No locked races without results -> main() returns normally (exit 0
        # by falling off the end, no SystemExit raised for a failure).
        try:
            fr.main()
        except SystemExit as e:
            assert e.code in (0, None)


class TestRaceManagerMainExitCode:
    def test_poll_for_results_returns_failed_race_names(self, tmp_path):
        import race_manager as rm

        db_path = str(tmp_path / 'f1.db')
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
        db.execute('''
            CREATE TABLE races (id INTEGER PRIMARY KEY, name TEXT NOT NULL, round INTEGER NOT NULL,
                date TEXT NOT NULL, status TEXT DEFAULT 'open', session_key INTEGER)
        ''')
        db.execute('''
            CREATE TABLE race_stages (race_id INTEGER PRIMARY KEY, stage TEXT NOT NULL,
                entered_at TEXT NOT NULL, last_poll_at TEXT, poll_count INTEGER DEFAULT 0)
        ''')
        # Race started 5h ago (past the 4h finished-long-ago threshold), but
        # this polling stage was only entered 30min ago — well under
        # MAX_POLL_DURATION (6h) — so the give-up path doesn't fire first.
        race_started = (datetime.now(timezone.utc) - timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
        entered_polling = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
        db.execute(
            "INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)",
            (1, 'Test GP', 1, race_started, 'locked', 9001),
        )
        db.execute(
            "INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count) "
            "VALUES (?, 'polling', ?, NULL, 0)",
            (1, entered_polling),
        )
        db.commit()

        with patch('race_manager.openf1.get_podium', return_value=None):
            failures = rm.poll_for_results(db, datetime.now(timezone.utc))

        assert failures == ['Test GP']
        row = db.execute("SELECT outcome FROM fetch_attempts WHERE race_id = 1").fetchone()
        assert row['outcome'] == 'empty'
        db.close()


class TestHealthEndpoint:
    def test_health_reports_no_fetch_yet(self, app, client):
        response = client.get('/health')
        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'healthy'
        assert data['last_successful_fetch_at'] is None
        assert data['last_successful_fetch_age_seconds'] is None

    def test_health_reports_last_successful_fetch_age(self, app, client):
        from app import get_db
        db = get_db()
        fa.record_fetch_attempt(db, 'session_result', 'k', fa.Outcome.OK, session_key=1, race_id=1)

        response = client.get('/health')
        data = response.get_json()
        assert data['status'] == 'healthy'
        assert data['last_successful_fetch_at'] is not None
        assert data['last_successful_fetch_age_seconds'] is not None
        assert data['last_successful_fetch_age_seconds'] >= 0
