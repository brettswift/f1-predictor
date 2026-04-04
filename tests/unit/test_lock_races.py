"""Unit tests for lock_races.py (BUD-74: CJ-8)."""

import pytest
import os
import sys
import yaml
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

# Set test environment BEFORE imports
os.environ['DATABASE_PATH'] = ':memory:'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))


class TestLockRacesLogic:
    """Test cases for race locking logic (CJ-015, CJ-016)."""

    @pytest.fixture
    def db_connection(self):
        """Create an in-memory database for testing."""
        import lock_races as lr
        conn = lr.sqlite3.connect(':memory:')
        conn.row_factory = lr.sqlite3.Row

        conn.execute('''
            CREATE TABLE races (
                id INTEGER PRIMARY KEY,
                name TEXT,
                round INTEGER,
                date TIMESTAMP,
                status TEXT DEFAULT 'open'
            )
        ''')
        conn.commit()

        original_get_db = lr.get_db
        lr.get_db = lambda: conn

        yield conn

        lr.get_db = original_get_db
        conn.close()

    def test_cj_015_locks_race_when_date_in_past(self, db_connection):
        """CJ-015: Locks races where date < now and status = 'open'.

        Given a race with status='open' and date in the past
        When lock_races is called
        Then the race is locked
        """
        import lock_races as lr

        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Past GP', 1, past, 'open')
        )
        db_connection.commit()

        count = lr.lock_races(db_connection)

        assert count == 1
        status = db_connection.execute('SELECT status FROM races WHERE id = 1').fetchone()['status']
        assert status == 'locked'

    def test_cj_015_does_not_lock_future_races(self, db_connection):
        """CJ-015: Does not lock races that haven't started yet.

        Given a race with status='open' and date in the future
        When lock_races is called
        Then the race is NOT locked
        """
        import lock_races as lr

        future = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Future GP', 1, future, 'open')
        )
        db_connection.commit()

        count = lr.lock_races(db_connection)

        assert count == 0
        status = db_connection.execute('SELECT status FROM races WHERE id = 1').fetchone()['status']
        assert status == 'open'

    def test_cj_015_does_not_re_lock_already_locked(self, db_connection):
        """CJ-015: Already locked races are not re-locked.

        Given a race with status='locked'
        When lock_races is called
        Then nothing happens (no error)
        """
        import lock_races as lr

        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Past GP', 1, past, 'locked')
        )
        db_connection.commit()

        count = lr.lock_races(db_connection)

        assert count == 0
        status = db_connection.execute('SELECT status FROM races WHERE id = 1').fetchone()['status']
        assert status == 'locked'

    def test_cj_015_locks_multiple_races(self, db_connection):
        """CJ-015: Multiple races can be locked at once.

        Given multiple races with past dates
        When lock_races is called
        Then all are locked
        """
        import lock_races as lr

        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Past GP 1', 1, past, 'open')
        )
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (2, 'Past GP 2', 2, past, 'open')
        )
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (3, 'Future GP', 3, (datetime.now(timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        db_connection.commit()

        count = lr.lock_races(db_connection)

        assert count == 2
        statuses = {r['id']: r['status'] for r in db_connection.execute('SELECT id, status FROM races').fetchall()}
        assert statuses[1] == 'locked'
        assert statuses[2] == 'locked'
        assert statuses[3] == 'open'

    def test_cj_016_logs_locked_race_name(self, db_connection, caplog):
        """CJ-016: Logs "Locked race: {name} (was open since {date})".

        Given a race that gets locked
        When lock_races is called
        Then the log contains the race name and original date
        """
        import lock_races as lr
        import logging

        past_date = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Bahrain Grand Prix', 1, past_date, 'open')
        )
        db_connection.commit()

        with caplog.at_level(logging.INFO):
            lr.lock_races(db_connection)

        assert any('Bahrain Grand Prix' in record.message and 'was open since' in record.message
                   for record in caplog.records), "Should log race name and original date"

    def test_cj_016_logs_count_when_no_races(self, db_connection, caplog):
        """CJ-016: Logs "No races to lock" when nothing to do.

        Given no races need locking
        When lock_races is called
        Then a "No races to lock" log is emitted
        """
        import lock_races as lr
        import logging

        future = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
        db_connection.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (1, 'Future GP', 1, future, 'open')
        )
        db_connection.commit()

        with caplog.at_level(logging.INFO):
            lr.lock_races(db_connection)

        assert any('No races to lock' in record.message for record in caplog.records)


class TestLockRacesCronJobManifest:
    """Test cases for the lock-races CronJob Kubernetes manifest."""

    @pytest.fixture
    def cronjob_spec(self):
        """Load the lock-races CronJob YAML."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'lock-races-cronjob.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_cj_015_cronjob_exists_and_valid(self, cronjob_spec):
        """CJ-015: Lock-races CronJob is a valid CronJob resource."""
        assert cronjob_spec['kind'] == 'CronJob'
        assert cronjob_spec['apiVersion'] == 'batch/v1'

    def test_cj_015_runs_every_minute(self, cronjob_spec):
        """CJ-015: CronJob runs every minute (* * * * *)."""
        assert cronjob_spec['spec']['schedule'] == "* * * * *"

    def test_cj_015_runs_lock_races_script(self, cronjob_spec):
        """CJ-015: CronJob runs lock_races.py."""
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        command_str = ' '.join(containers[0]['command'])
        assert 'lock_races.py' in command_str

    def test_cj_015_restart_policy_on_failure(self, cronjob_spec):
        """CJ-015: CronJob has restartPolicy: OnFailure."""
        restart = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['restartPolicy']
        assert restart == 'OnFailure'

    def test_cj_015_resource_limits_set(self, cronjob_spec):
        """CJ-015: CronJob has resource limits."""
        resources = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['resources']
        assert 'requests' in resources
        assert 'limits' in resources

    def test_cj_015_pvc_mounted_at_data(self, cronjob_spec):
        """CJ-015: PVC mounted at /data."""
        mounts = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['volumeMounts']
        data_mount = next((m for m in mounts if m['mountPath'] == '/data'), None)
        assert data_mount is not None

    def test_cj_015_concurrency_policy_forbid(self, cronjob_spec):
        """CJ-015: ConcurrencyPolicy is Forbid."""
        assert cronjob_spec['spec'].get('concurrencyPolicy') == 'Forbid'

    def test_cj_015_failed_jobs_history_limit(self, cronjob_spec):
        """CJ-015: Failed jobs history is retained."""
        assert cronjob_spec['spec'].get('failedJobsHistoryLimit') == 2
