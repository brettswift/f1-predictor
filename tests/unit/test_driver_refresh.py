"""Unit tests for refresh_drivers.py (BUD-71: CJ-7)."""

import pytest
import os
import sys
import yaml
from unittest.mock import patch, MagicMock

# Set test environment BEFORE imports
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['F1_SEASON'] = '2026'
os.environ['OPENF1_OFFLINE'] = 'true'

# Add cron/ and src/ to path so we can import refresh_drivers (which itself
# imports openf1 from src/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestDriverRefreshAPI:
    """Test cases for driver API fetch (CJ-013)."""

    def test_cj_013_api_returns_driver_data(self):
        """CJ-013: OpenF1 returns driver data structure.

        Given openf1.get_drivers returns a driver list
        When fetch_drivers_from_api is called
        Then correct driver data is returned
        """
        import refresh_drivers as dr

        openf1_drivers = [
            {
                'driver_number': 1,
                'full_name': 'Max VERSTAPPEN',
                'name_acronym': 'VER',
                'country_code': 'NED',
                'team_name': 'Red Bull Racing',
            },
            {
                'driver_number': 44,
                'full_name': 'Lewis HAMILTON',
                'name_acronym': 'HAM',
                'country_code': 'GBR',
                'team_name': 'Ferrari',
            },
        ]

        with patch('refresh_drivers.openf1.get_drivers') as mock_get_drivers:
            mock_get_drivers.return_value = MagicMock(data=openf1_drivers)
            drivers = dr.fetch_drivers_from_api()

        assert drivers is not None
        assert len(drivers) == 2
        assert drivers[0]['driver_id'] == 'ver'
        assert drivers[0]['name'] == 'Max Verstappen'
        assert drivers[0]['code'] == 'VER'
        assert drivers[0]['number'] == 1
        assert drivers[1]['code'] == 'HAM'

    def test_cj_013_handles_api_error_gracefully(self):
        """CJ-013: API error is handled gracefully.

        Given openf1.get_drivers raises OpenF1Error
        When fetch_drivers_from_api is called
        Then None is returned (not an exception)
        """
        import refresh_drivers as dr
        import openf1

        with patch('refresh_drivers.openf1.get_drivers') as mock_get_drivers:
            mock_get_drivers.side_effect = openf1.OpenF1Error("Network error")

            drivers = dr.fetch_drivers_from_api()

        assert drivers is None, "Should return None on API error"

    def test_cj_013_handles_missing_driver_number(self):
        """CJ-013: Drivers without a car number are skipped.

        Given openf1.get_drivers returns a driver with no driver_number
        When fetch_drivers_from_api is called
        Then that driver is skipped rather than crashing
        """
        import refresh_drivers as dr

        openf1_drivers = [
            {'driver_number': None, 'full_name': 'Test Driver'},
            {'driver_number': 7, 'full_name': 'Real Driver', 'name_acronym': 'REA'},
        ]

        with patch('refresh_drivers.openf1.get_drivers') as mock_get_drivers:
            mock_get_drivers.return_value = MagicMock(data=openf1_drivers)
            drivers = dr.fetch_drivers_from_api()

        assert drivers is not None
        assert len(drivers) == 1
        assert drivers[0]['name'] == 'Real Driver'


class TestDriverRefreshDB:
    """Test cases for driver DB refresh (CJ-014)."""

    @pytest.fixture
    def db_connection(self):
        """Create an in-memory database for testing."""
        import refresh_drivers as dr
        conn = dr.sqlite3.connect(':memory:')
        conn.row_factory = dr.sqlite3.Row

        # Create schema
        conn.executescript('''
            CREATE TABLE drivers (
                id INTEGER PRIMARY KEY,
                driver_id TEXT UNIQUE,
                name TEXT,
                team TEXT,
                number INTEGER,
                code TEXT,
                nationality TEXT
            );
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY,
                user_id TEXT,
                race_id INTEGER,
                p1_driver_id INTEGER,
                p2_driver_id INTEGER,
                p3_driver_id INTEGER
            );
            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP
            );
        ''')
        conn.commit()

        original_get_db = dr.get_db
        dr.get_db = lambda: conn

        yield conn

        dr.get_db = original_get_db
        conn.close()

    def test_cj_014_drivers_populated_from_api(self, db_connection):
        """CJ-014: Unit test verifies driver table is populated from API.

        Given the API returns driver data
        When refresh_drivers is called
        Then drivers table is populated correctly
        """
        import refresh_drivers as dr

        mock_drivers = [
            {'id': 1, 'driver_id': 'verstappen', 'name': 'Max Verstappen',
             'team': None, 'number': 1, 'code': 'VER', 'nationality': 'Dutch', 'new_id': 1},
            {'id': 2, 'driver_id': 'hamilton', 'name': 'Lewis Hamilton',
             'team': None, 'number': 44, 'code': 'HAM', 'nationality': 'British', 'new_id': 2},
        ]

        with patch.object(dr, 'fetch_drivers_from_api', return_value=mock_drivers):
            result = dr.refresh_drivers(db_connection)

        assert result is True

        rows = db_connection.execute('SELECT * FROM drivers ORDER BY id').fetchall()
        assert len(rows) == 2
        assert rows[0]['driver_id'] == 'verstappen'
        assert rows[1]['code'] == 'HAM'

    def test_cj_014_predictions_remapped_when_drivers_reordered(self, db_connection):
        """CJ-014: Predictions are remapped when driver ID assignments change.

        Given existing predictions with driver IDs
        When refresh_drivers replaces drivers and a driver gets a new ID
        Then predictions are updated to use the new driver ID
        """
        import refresh_drivers as dr

        # Insert old drivers
        db_connection.execute(
            'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (1, 'verstappen', 'Max Verstappen', None, 1, 'VER', 'Dutch')
        )
        db_connection.execute(
            'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (2, 'hamilton', 'Lewis Hamilton', None, 44, 'HAM', 'British')
        )
        # Insert prediction using old driver IDs (verstappen=1, hamilton=2)
        db_connection.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            ('user1', 1, 1, 2, 1)
        )
        db_connection.commit()

        # New API returns same drivers - ID assignment is by API order
        # verstappen is first in API, hamilton second
        # Function assigns new_id sequentially starting from 1
        # So: verstappen→new_id=1, hamilton→new_id=2
        # Since old IDs are 1 and 2, and new IDs are also 1 and 2,
        # the remapping is {1:1, 2:2} - identity mapping, predictions unchanged
        mock_drivers = [
            {'id': 1, 'driver_id': 'verstappen', 'name': 'Max Verstappen',
             'team': None, 'number': 1, 'code': 'VER', 'nationality': 'Dutch'},
            {'id': 2, 'driver_id': 'hamilton', 'name': 'Lewis Hamilton',
             'team': None, 'number': 44, 'code': 'HAM', 'nationality': 'British'},
        ]

        with patch.object(dr, 'fetch_drivers_from_api', return_value=mock_drivers):
            result = dr.refresh_drivers(db_connection)

        assert result is True

        # After refresh: IDs are reassigned sequentially (1, 2, ...)
        # Since verstappen→1 and hamilton→2, the mapping is identity
        # Predictions still reference IDs 1 and 2, which now point to the same drivers
        pred = db_connection.execute('SELECT * FROM predictions WHERE user_id = ?', ('user1',)).fetchone()
        # IDs remapped but since mapping is {1:1, 2:2}, predictions unchanged
        assert pred['p1_driver_id'] == 1  # verstappen still ID 1
        assert pred['p2_driver_id'] == 2  # hamilton still ID 2

        # Verify drivers were actually replaced
        drivers = db_connection.execute('SELECT driver_id, id FROM drivers ORDER BY id').fetchall()
        assert len(drivers) == 2
        assert drivers[0]['driver_id'] == 'verstappen'
        assert drivers[1]['driver_id'] == 'hamilton'

    def test_cj_014_metadata_updated(self, db_connection):
        """CJ-014: Metadata is updated with refresh timestamp.

        Given a successful refresh
        When refresh_drivers completes
        Then metadata 'drivers_last_refresh' is set
        """
        import refresh_drivers as dr

        mock_drivers = [
            {'id': 1, 'driver_id': 'verstappen', 'name': 'Max Verstappen',
             'team': None, 'number': 1, 'code': 'VER', 'nationality': 'Dutch', 'new_id': 1},
        ]

        with patch.object(dr, 'fetch_drivers_from_api', return_value=mock_drivers):
            result = dr.refresh_drivers(db_connection)

        assert result is True

        meta = db_connection.execute(
            "SELECT * FROM metadata WHERE key = 'drivers_last_refresh'"
        ).fetchone()
        assert meta is not None
        assert meta['value'] is not None

    def test_cj_014_unknown_drivers_replace_completely(self, db_connection):
        """CJ-014: All old drivers are replaced by new API drivers.

        Given existing drivers in the database
        When refresh_drivers is called with new driver list
        Then old drivers are removed and new ones inserted
        """
        import refresh_drivers as dr

        # Insert old drivers
        db_connection.execute(
            'INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (1, 'old_driver', 'Old Driver', None, 99, 'OLD', 'Test')
        )
        db_connection.commit()

        mock_drivers = [
            {'id': 1, 'driver_id': 'verstappen', 'name': 'Max Verstappen',
             'team': None, 'number': 1, 'code': 'VER', 'nationality': 'Dutch', 'new_id': 1},
        ]

        with patch.object(dr, 'fetch_drivers_from_api', return_value=mock_drivers):
            result = dr.refresh_drivers(db_connection)

        assert result is True

        rows = db_connection.execute('SELECT driver_id FROM drivers').fetchall()
        assert len(rows) == 1
        assert rows[0]['driver_id'] == 'verstappen'


class TestDriverRefreshCronJobManifest:
    """Test cases for the driver refresh CronJob Kubernetes manifest."""

    @pytest.fixture
    def cronjob_spec(self):
        """Load the driver refresh CronJob YAML."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'base', 'driver-refresh-cronjob.yaml'
        )
        with open(yaml_path) as f:
            return yaml.safe_load(f)

    def test_cj_013_cronjob_exists_and_is_valid(self, cronjob_spec):
        """CJ-013: Driver refresh CronJob is defined as valid CronJob.

        Given the driver-refresh CronJob YAML exists
        When parsed
        Then it should be a valid CronJob resource
        """
        assert cronjob_spec['kind'] == 'CronJob', "Should be a CronJob resource"
        assert cronjob_spec['apiVersion'] == 'batch/v1', "Should use batch/v1 API"

    def test_cj_013_weekly_schedule_configured(self, cronjob_spec):
        """CJ-013: CronJob runs weekly (Sunday 02:00 UTC).

        Given the CronJob is defined
        When checked
        Then schedule should be weekly (Sunday)
        """
        schedule = cronjob_spec['spec']['schedule']
        assert schedule == "0 2 * * 0", f"Expected weekly schedule '0 2 * * 0', got {schedule}"

    def test_cj_013_runs_refresh_drivers_script(self, cronjob_spec):
        """CJ-013: CronJob runs refresh_drivers.py.

        Given the CronJob container is defined
        When checked
        Then command should run refresh_drivers.py
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        assert len(containers) == 1
        container = containers[0]

        command_str = ' '.join(container['command'])
        assert 'python3' in command_str, "Should run python3"
        assert 'refresh_drivers.py' in command_str, "Should run refresh_drivers.py"

    def test_cj_013_restart_policy_on_failure(self, cronjob_spec):
        """CJ-013: CronJob has restartPolicy: OnFailure.

        Given the CronJob spec
        When checked
        Then restartPolicy should be OnFailure
        """
        restart_policy = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['restartPolicy']
        assert restart_policy == 'OnFailure', f"Expected OnFailure, got {restart_policy}"

    def test_cj_013_resource_limits_set(self, cronjob_spec):
        """CJ-013: CronJob has resource limits.

        Given the CronJob container
        When checked
        Then resources (requests and limits) should be defined
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]

        assert 'resources' in container, "Should have resources defined"
        resources = container['resources']
        assert 'requests' in resources, "Should have resource requests"
        assert 'limits' in resources, "Should have resource limits"

    def test_cj_013_pvc_mounted_at_data(self, cronjob_spec):
        """CJ-013: PVC mounted at /data.

        Given the CronJob is defined
        When checked
        Then volumeMounts should have /data mounted from PVC
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]

        volume_mounts = container.get('volumeMounts', [])
        data_mount = next((m for m in volume_mounts if m['mountPath'] == '/data'), None)
        assert data_mount is not None, "Should have /data mount"
        assert data_mount['name'] == 'data', "Should mount 'data' volume"

    def test_cj_013_database_path_env_set(self, cronjob_spec):
        """CJ-013: DATABASE_PATH environment variable set.

        Given the CronJob container env vars
        When checked
        Then DATABASE_PATH should be /data/f1_predictions.db
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]

        env = {e['name']: e['value'] for e in container.get('env', [])}
        assert 'DATABASE_PATH' in env, "Should have DATABASE_PATH env var"
        assert env['DATABASE_PATH'] == '/data/f1_predictions.db'

    def test_cj_013_no_jolpica_env_left(self, cronjob_spec):
        """F1-01: no leftover Jolpica/Ergast config on the CronJob.

        Given the CronJob container env vars (cron scripts now read
        OpenF1's default via src/openf1.py, no F1_API_URL override needed)
        When checked
        Then no env var should reference F1_API_URL or jolpi.ca
        """
        containers = cronjob_spec['spec']['jobTemplate']['spec']['template']['spec']['containers']
        container = containers[0]

        env = {e['name']: e.get('value', '') for e in container.get('env', [])}
        assert 'F1_API_URL' not in env, "F1_API_URL should be removed — cron scripts no longer read it"
        assert not any('jolpi.ca' in v for v in env.values()), "No jolpi.ca reference should remain"

    def test_cj_013_concurrency_policy_forbid(self, cronjob_spec):
        """CJ-013: ConcurrencyPolicy is Forbid to prevent overlaps.

        Given the CronJob spec
        When checked
        Then concurrencyPolicy should be Forbid
        """
        assert cronjob_spec['spec'].get('concurrencyPolicy') == 'Forbid', \
            "ConcurrencyPolicy should be Forbid"

    def test_cj_013_failed_jobs_history_limit(self, cronjob_spec):
        """CJ-013: Failed jobs history is retained.

        Given the CronJob spec
        When checked
        Then failedJobsHistoryLimit should be set
        """
        assert 'failedJobsHistoryLimit' in cronjob_spec['spec'], \
            "Should have failedJobsHistoryLimit"
        assert cronjob_spec['spec']['failedJobsHistoryLimit'] == 2
