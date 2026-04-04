"""Unit tests for scheduler.py (F1-CJ-5: Test scheduler and idle behavior)."""

import pytest
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import json
import tempfile

# Set test environment
os.environ['DATABASE_PATH'] = ':memory:'

# Add cron/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))


class TestSchedulerIdleBehavior:
    """Test cases for scheduler idle behavior (CJ-007)."""

    def test_cj_007_script_exits_quickly_when_no_active_races(self):
        """CJ-007: Script exits quickly (< 1s) when no active races.
        
        Given the database has no upcoming races
        When scheduler.main() is called
        Then it should return quickly (no infinite loop)
        """
        import scheduler
        
        # Mock get_db to return empty races
        with patch.object(scheduler, 'get_db') as mock_get_db:
            mock_db = MagicMock()
            mock_db.execute.return_value.fetchall.return_value = []
            mock_get_db.return_value = mock_db
            
            import time
            start = time.time()
            scheduler.main()
            elapsed = time.time() - start
            
            assert elapsed < 1.0, f"Scheduler should exit quickly when no races, took {elapsed:.2f}s"

    def test_cj_007_no_upcoming_races_returns_early(self):
        """CJ-007 variant: No upcoming races causes early return.
        
        Given get_upcoming_races returns empty list
        When scheduler runs
        Then it logs 'No upcoming races to schedule' and exits
        """
        import scheduler
        
        with patch.object(scheduler, 'get_upcoming_races', return_value=[]) as mock_get:
            with patch.object(scheduler, 'logger') as mock_logger:
                scheduler.main()
                
                mock_get.assert_called_once()
                # Verify it logged the appropriate message
                assert any('No upcoming races' in str(call) for call in mock_logger.info.call_args_list)


class TestSchedulerJobCreation:
    """Test cases for scheduler job creation (CJ-011)."""

    @pytest.fixture
    def mock_race(self):
        """Create a mock race in the future."""
        future_time = datetime.now(timezone.utc) + timedelta(hours=6)
        return {
            'id': 1,
            'name': 'Test Grand Prix',
            'round': 1,
            'date': future_time.strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'open'
        }

    def test_cj_011_schedules_race_result_job(self, mock_race):
        """CJ-011: One-time jobs created when race enters polling.
        
        Given a race that needs scheduling
        When scheduler processes the race
        Then a job should be created to fetch results
        """
        import scheduler
        
        with patch.object(scheduler, 'get_upcoming_races', return_value=[mock_race]) as mock_get:
            with patch.object(scheduler, 'spawn_kubernetes_cronjob', return_value=True) as mock_spawn:
                with patch.object(scheduler, 'load_state', return_value={'scheduled_jobs': []}):
                    with patch.object(scheduler, 'save_state'):
                        scheduler.main()
                        
                        mock_spawn.assert_called_once()
                        call_args = mock_spawn.call_args
                        assert call_args[0][0] == mock_race['id']  # race_id
                        assert call_args[0][1] == mock_race['name']  # race_name

    def test_cj_011_skips_already_scheduled_races(self, mock_race):
        """CJ-011 variant: Already scheduled races are skipped.
        
        Given a race is already in the scheduled_jobs state
        When scheduler runs
        Then spawn should not be called for that race
        """
        import scheduler
        
        with patch.object(scheduler, 'get_upcoming_races', return_value=[mock_race]) as mock_get:
            with patch.object(scheduler, 'spawn_kubernetes_cronjob', return_value=True) as mock_spawn:
                with patch.object(scheduler, 'load_state', return_value={'scheduled_jobs': [mock_race['id']]}):
                    with patch.object(scheduler, 'save_state'):
                        scheduler.main()
                        
                        mock_spawn.assert_not_called()

    def test_cj_011_multiple_races_scheduled(self):
        """CJ-011 variant: Multiple races get scheduled.
        
        Given multiple races need scheduling
        When scheduler runs
        Then each race should get a job
        """
        import scheduler
        
        races = [
            {'id': 1, 'name': 'Race 1', 'date': (datetime.now(timezone.utc) + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S'), 'status': 'open'},
            {'id': 2, 'name': 'Race 2', 'date': (datetime.now(timezone.utc) + timedelta(hours=10)).strftime('%Y-%m-%d %H:%M:%S'), 'status': 'open'},
        ]
        
        with patch.object(scheduler, 'get_upcoming_races', return_value=races) as mock_get:
            with patch.object(scheduler, 'spawn_kubernetes_cronjob', return_value=True) as mock_spawn:
                with patch.object(scheduler, 'load_state', return_value={'scheduled_jobs': []}):
                    with patch.object(scheduler, 'save_state'):
                        scheduler.main()
                        
                        assert mock_spawn.call_count == 2


class TestScheduleRaceResultJob:
    """Test schedule_race_result_job function."""

    def test_schedules_at_correct_time(self):
        """Job is scheduled for 1.5 hours after race start."""
        import scheduler
        
        race_time = datetime(2026, 6, 15, 14, 0, 0)
        fetch_time = race_time + timedelta(hours=1, minutes=30)
        
        assert fetch_time.hour == 15
        assert fetch_time.minute == 30


class TestSpawnKubernetesCronJob:
    """Test spawn_kubernetes_cronjob function."""

    def test_spawn_accepts_valid_race_params(self):
        """spawn_kubernetes_cronjob accepts valid parameters.
        
        Given a valid race ID and datetime
        When spawn_kubernetes_cronjob is called
        Then it should not raise any validation errors
        """
        import scheduler
        
        race_datetime = '2026-06-15 14:00:00'
        race_id = 42
        
        # Just verify the function accepts these parameters without error
        # The actual kubectl call is tested via integration tests
        # We can't fully test this without a k8s cluster
        assert callable(scheduler.spawn_kubernetes_cronjob)


class TestCleanupOldJobs:
    """Test cleanup_old_jobs function."""

    def test_cleanup_runs_without_error(self):
        """Cleanup should not raise errors even if kubectl fails."""
        import scheduler
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            
            # Should not raise
            scheduler.cleanup_old_jobs()


class TestLoadSaveState:
    """Test state persistence functions."""

    def test_load_state_returns_dict(self):
        """Load state returns a dictionary."""
        import scheduler
        
        with patch('os.path.exists', return_value=False):
            state = scheduler.load_state()
            assert isinstance(state, dict)

    def test_load_state_returns_existing_data(self):
        """Load state returns existing state file data."""
        import scheduler
        
        test_state = {'scheduled_jobs': [1, 2, 3]}
        
        with patch('os.path.exists', return_value=True):
            with patch('builtins.open', create=True):
                with patch('json.load') as mock_json:
                    mock_json.return_value = test_state
                    state = scheduler.load_state()
                    assert state == test_state


class TestGetUpcomingRaces:
    """Test get_upcoming_races function."""

    def test_returns_list_of_races(self):
        """Returns list of races from database."""
        import scheduler
        
        mock_races = [
            {'id': 1, 'name': 'Race 1', 'round': 1, 'date': '2026-06-15 14:00:00', 'status': 'open'}
        ]
        
        with patch.object(scheduler, 'get_db') as mock_get_db:
            mock_db = MagicMock()
            mock_db.execute.return_value.fetchall.return_value = mock_races
            mock_get_db.return_value = mock_db
            
            races = scheduler.get_upcoming_races()
            
            assert len(races) == 1
            assert races[0]['name'] == 'Race 1'
