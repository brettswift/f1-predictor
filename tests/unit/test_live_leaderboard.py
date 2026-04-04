"""Unit tests for live leaderboard feature (BUD-52: LL-001 to LL-011)."""

import pytest
import os
import uuid
from datetime import datetime, timezone


class TestLiveLeaderboardAccess:
    """Test cases for LL-001: /race/<id>/live page accessible during race."""

    def _setup_locked_race(self, db, race_id=1):
        """Set up a locked race for testing."""
        session_id = f'test-user-{race_id}'
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'testuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test Grand Prix {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+1, f'verstappen{race_id}', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+2, f'hamilton{race_id}', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+3, f'norris{race_id}', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        return session_id

    def test_ll_001_live_page_accessible_for_locked_race(self, app, client):
        """LL-001: /race/<id>/live should be accessible during locked race."""
        from app import get_db
        db = get_db()
        session_id = self._setup_locked_race(db, race_id=101)
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get('/race/101/live')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

    def test_ll_001_live_page_redirects_for_completed_race(self, app, client):
        """LL-001: /race/<id>/live should redirect to race detail for completed race."""
        from app import get_db
        db = get_db()
        session_id = f'test-user-completed'
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, 'testusercompleted'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (102, 'Completed GP', 2, '2026-03-01 14:00:00', 'completed'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (1021, 'verstappen102', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (1022, 'hamilton102', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (1023, 'norris102', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.execute('INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
                   (102, 1021, 1022, 1023))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get('/race/102/live', follow_redirects=False)
        assert response.status_code == 302, "Completed race should redirect"

    def test_ll_001_live_page_redirects_for_open_race(self, app, client):
        """LL-001: /race/<id>/live should redirect for open (future) race."""
        from app import get_db
        db = get_db()
        session_id = f'test-user-open'
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, 'testuseropen'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (103, 'Future GP', 3, '2026-12-01 14:00:00', 'open'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get('/race/103/live')
        assert response.status_code == 302, "Open race should redirect away from live page"

    def test_ll_001_live_page_requires_login(self, client):
        """LL-001: /race/<id>/live should require login."""
        response = client.get('/race/1/live', follow_redirects=True)
        assert 'username' in response.data.decode('utf-8').lower() or response.status_code == 302


class TestProjectedPoints:
    """Test cases for LL-003: Projected points calculation."""

    def test_ll_003_exact_position_match(self, app):
        """LL-003: Correct driver in correct position should earn full points."""
        from app import calculate_projected_points
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        current_positions = [
            {'driver_id': '1', 'position': 1},
            {'driver_id': '2', 'position': 2},
            {'driver_id': '3', 'position': 3},
        ]
        
        result = calculate_projected_points(prediction, current_positions)
        
        assert result['p1_match'] == True
        assert result['p2_match'] == True
        assert result['p3_match'] == True
        assert result['projected_points'] == 20  # 10 + 6 + 4

    def test_ll_003_driver_match_wrong_position(self, app):
        """LL-003: Correct driver in wrong position should earn 1 point."""
        from app import calculate_projected_points
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        # P1 predicted driver is in P2, P2 predicted is in P1, P3 predicted is in P3
        current_positions = [
            {'driver_id': '2', 'position': 1},
            {'driver_id': '1', 'position': 2},
            {'driver_id': '3', 'position': 3},
        ]
        
        result = calculate_projected_points(prediction, current_positions)
        
        assert result['p1_match'] == False
        assert result['p2_match'] == False
        assert result['p3_match'] == True
        assert result['driver_matches'] == 2  # 2 drivers in podium but wrong position
        assert result['projected_points'] == 6  # 4 for P3 exact + 1 + 1

    def test_ll_003_no_matches(self, app):
        """LL-003: No matches should earn 0 points."""
        from app import calculate_projected_points
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        # All predicted drivers outside podium
        current_positions = [
            {'driver_id': '4', 'position': 1},
            {'driver_id': '5', 'position': 2},
            {'driver_id': '6', 'position': 3},
        ]
        
        result = calculate_projected_points(prediction, current_positions)
        
        assert result['p1_match'] == False
        assert result['p2_match'] == False
        assert result['p3_match'] == False
        assert result['projected_points'] == 0

    def test_ll_003_partial_podium(self, app):
        """LL-003: Some predicted drivers in podium."""
        from app import calculate_projected_points
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        # Only P1 predicted is in podium at correct position
        current_positions = [
            {'driver_id': '1', 'position': 1},
            {'driver_id': '4', 'position': 2},
            {'driver_id': '5', 'position': 3},
        ]
        
        result = calculate_projected_points(prediction, current_positions)
        
        assert result['p1_match'] == True
        assert result['p2_match'] == False
        assert result['p3_match'] == False
        assert result['projected_points'] == 10  # Only P1 exact

    def test_ll_003_empty_positions(self, app):
        """LL-003: Empty positions should return 0 points."""
        from app import calculate_projected_points
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        result = calculate_projected_points(prediction, None)
        
        assert result['projected_points'] == 0


class TestBestWorstCase:
    """Test cases for LL-005: Best/worst case projection."""

    def test_ll_005_all_correct(self, app):
        """LL-005: All predictions correct should have best=worst=current."""
        from app import calculate_best_worst_case
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        current_positions = [
            {'driver_id': '1', 'position': 1},
            {'driver_id': '2', 'position': 2},
            {'driver_id': '3', 'position': 3},
        ]
        
        result = calculate_best_worst_case(prediction, current_positions)
        
        assert result['best'] == 20
        assert result['worst'] == 20
        assert result['current'] == 20

    def test_ll_005_partial_correct(self, app):
        """LL-005: Partial correct predictions."""
        from app import calculate_best_worst_case
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        # P1 predicted (driver 1) is at P1 (exact)
        # P2 predicted (driver 2) is at P4 (outside podium)
        # P3 predicted (driver 3) is at P5 (outside podium)
        current_positions = [
            {'driver_id': '1', 'position': 1},
            {'driver_id': '4', 'position': 2},
            {'driver_id': '5', 'position': 3},
        ]
        
        result = calculate_best_worst_case(prediction, current_positions)
        
        # Current: P1 exact = 10, P2 outside podium = 0, P3 outside podium = 0
        assert result['current'] == 10
        # Best: P1 already exact (0 remaining), P2 could get 6 more, P3 could get 4 more
        assert result['best'] == 20
        # Worst: stay at 10
        assert result['worst'] == 10

    def test_ll_005_none_in_podium(self, app):
        """LL-005: No predicted drivers in podium yet."""
        from app import calculate_best_worst_case
        
        prediction = {
            'p1_driver_id': '1',
            'p2_driver_id': '2',
            'p3_driver_id': '3',
        }
        
        current_positions = [
            {'driver_id': '4', 'position': 1},
            {'driver_id': '5', 'position': 2},
            {'driver_id': '6', 'position': 3},
        ]
        
        result = calculate_best_worst_case(prediction, current_positions)
        
        assert result['current'] == 0
        # Best: could get all 3 exact = 20
        assert result['best'] == 20
        # Worst: 0
        assert result['worst'] == 0


class TestLiveDataCaching:
    """Test cases for LL-007: Rate limiting via caching."""

    def test_ll_007_cache_hit(self, app):
        """LL-007: Cached data should be returned within TTL."""
        from app import _set_cached_live_data, _live_data_cache
        
        # Set cache
        test_data = [{'driver_id': '1', 'position': 1}]
        race_id = 9999
        _set_cached_live_data(race_id, test_data)
        
        # Should return cached data
        assert race_id in _live_data_cache
        assert _live_data_cache[race_id]['data'] == test_data

    def test_ll_007_cache_miss_for_new_race(self, app):
        """LL-007: Cache should be empty for new race_id."""
        from app import _get_cached_live_data, _live_data_cache
        
        # Clear any existing cache entries for this test
        test_race_id = 99999
        # Make sure it's not in cache
        if test_race_id in _live_data_cache:
            del _live_data_cache[test_race_id]
        
        result = _get_cached_live_data(test_race_id)
        assert result is None


class TestGracefulFallback:
    """Test cases for LL-008: Graceful fallback if API unavailable."""

    def test_ll_008_api_unavailable_shows_warning(self, app, client, monkeypatch):
        """LL-008: When API is unavailable, show warning message."""
        from app import get_db
        db = get_db()
        
        race_id = 801
        session_id = f'test-user-{race_id}'
        
        # Setup
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'testuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                   (session_id, race_id, 8011, 8012, 8013))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (8011, 'verstappen801', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (8012, 'hamilton801', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (8013, 'norris801', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        
        # Mock fetch_live_race_data to return None (API unavailable)
        import app as app_module
        original_fetch = app_module.fetch_live_race_data
        app_module.fetch_live_race_data = lambda s, r: None
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        try:
            response = client.get(f'/race/{race_id}/live')
            assert response.status_code == 200
            
            content = response.data.decode('utf-8')
            # Should show warning about API unavailability
            assert 'unavailable' in content.lower() or 'warning' in content.lower() or 'API' in content
        finally:
            app_module.fetch_live_race_data = original_fetch


class TestAutoRefresh:
    """Test cases for LL-002: Auto-refresh every 30 seconds."""

    def test_ll_002_auto_refresh_script_present(self, app, client):
        """LL-002: Template should include auto-refresh JavaScript."""
        from app import get_db
        db = get_db()
        
        race_id = 901
        session_id = f'test-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'testuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get(f'/race/{race_id}/live')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        # Should have auto-refresh timeout
        assert 'setTimeout' in content or 'refresh' in content.lower()
        # Should reference the refresh interval
        assert '30000' in content or 'refresh_interval' in content


class TestDriverTracker:
    """Test cases for LL-006: Driver tracker."""

    def test_ll_006_driver_tracker_shows_positions(self, app, client):
        """LL-006: Driver tracker should show current positions of predicted drivers."""
        from app import get_db
        db = get_db()
        
        race_id = 701
        session_id = f'tracker-user-{race_id}'
        
        # Setup user with prediction
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'trackuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                   (session_id, race_id, 7011, 7012, 7013))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (7011, 'verstappen701', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (7012, 'hamilton701', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (7013, 'norris701', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get(f'/race/{race_id}/live')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        # Should show driver tracker section
        assert 'Driver Tracker' in content or 'driver-tracker' in content


class TestPositionHighlights:
    """Test cases for LL-004: Position change highlights."""

    def test_ll_004_podium_positions_colored(self, app, client):
        """LL-004: Top 3 positions should have special styling."""
        from app import get_db
        db = get_db()
        
        race_id = 601
        session_id = f'test-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'testuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = session_id
        
        response = client.get(f'/race/{race_id}/live')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        # Check for position-related CSS classes
        assert 'position-1' in content or 'position-2' in content or 'position-3' in content or 'position-other' in content
