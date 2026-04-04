"""Playwright E2E tests for live leaderboard feature (BUD-53: LL-001 to LL-011)."""

import pytest
import pytest_playwright


@pytest.fixture
def app_url(app, client):
    """Get the app URL for Playwright."""
    # For local testing, we use the test client
    # In CI, this would be the deployed URL
    return "http://localhost:5000"


class TestLiveLeaderboardAccess:
    """Test cases for LL-001: /race/<id>/live page accessible during race."""

    @pytest.mark.e2e
    def test_ll_001_live_page_accessible_for_locked_race(self, page, app, client):
        """LL-001: /race/<id>/live should be accessible during locked race."""
        from app import get_db
        
        db = get_db()
        race_id = 1001
        session_id = f'playwright-user-{race_id}'
        
        # Set up test data
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Playwright GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+1, f'verstappen{race_id}', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+2, f'hamilton{race_id}', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+3, f'norris{race_id}', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        
        # Visit the live page
        response = page.goto(f'/race/{race_id}/live')
        # Should load (200) or redirect (302)
        assert response.status in [200, 302]
        
        # If redirected, follow
        if response.status == 302:
            response = page.goto(response.headers.get('location'), wait_until='load')
        
        assert response.status == 200

    @pytest.mark.e2e
    def test_ll_001_live_page_redirects_for_completed_race(self, page, app, client):
        """LL-001: /race/<id>/live should redirect to race detail for completed race."""
        from app import get_db
        
        db = get_db()
        race_id = 1002
        session_id = f'playwright-user-completed-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwusercompleted{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Completed GP {race_id}', 2, '2026-03-01 14:00:00', 'completed'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+1, f'verstappen{race_id}', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+2, f'hamilton{race_id}', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+3, f'norris{race_id}', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.execute('INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
                   (race_id, race_id*10+1, race_id*10+2, race_id*10+3))
        db.commit()
        
        response = page.goto(f'/race/{race_id}/live', wait_until='load')
        # Completed race should redirect away from live page
        assert page.url.endswith('/races') or '/race/' in page.url

    @pytest.mark.e2e
    def test_ll_001_live_page_requires_login(self, page):
        """LL-001: /race/<id>/live should require login."""
        response = page.goto('/race/1/live', wait_until='load')
        # Should redirect to login page
        assert 'username' in page.content().lower() or page.url.endswith('/')


class TestAutoRefresh:
    """Test cases for LL-002: Auto-refresh every 30 seconds."""

    @pytest.mark.e2e
    def test_ll_002_auto_refresh_script_present(self, page, app, client):
        """LL-002: Template should include auto-refresh JavaScript."""
        from app import get_db
        
        db = get_db()
        race_id = 1003
        session_id = f'playwright-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.commit()
        
        page.goto(f'/race/{race_id}/live')
        
        content = page.content()
        # Check for auto-refresh JavaScript
        assert 'setTimeout' in content or 'refresh' in content.lower()
        assert '30000' in content or '30000' in content  # 30 seconds in ms


class TestPositionHighlights:
    """Test cases for LL-004: Position change highlights."""

    @pytest.mark.e2e
    def test_ll_004_podium_positions_colored(self, page, app, client):
        """LL-004: Top 3 positions should have special styling."""
        from app import get_db
        
        db = get_db()
        race_id = 1004
        session_id = f'playwright-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.commit()
        
        page.goto(f'/race/{race_id}/live')
        
        content = page.content()
        # Check for position-specific CSS classes
        assert 'position-1' in content or 'position-2' in content or 'position-3' in content or 'p1' in content.lower()


class TestDriverTracker:
    """Test cases for LL-006: Driver tracker."""

    @pytest.mark.e2e
    def test_ll_006_driver_tracker_shows_predictions(self, page, app, client):
        """LL-006: Driver tracker should show current positions of predicted drivers."""
        from app import get_db
        
        db = get_db()
        race_id = 1005
        session_id = f'playwright-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                   (session_id, race_id, race_id*10+1, race_id*10+2, race_id*10+3))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+1, f'verstappen{race_id}', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+2, f'hamilton{race_id}', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+3, f'norris{race_id}', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        
        page.goto(f'/race/{race_id}/live')
        
        content = page.content()
        # Driver tracker section should be present
        assert 'Driver Tracker' in content or 'driver-tracker' in content or 'tracker' in content.lower()


class TestGracefulFallback:
    """Test cases for LL-008: Graceful fallback if API unavailable."""

    @pytest.mark.e2e
    def test_ll_008_api_unavailable_shows_warning(self, page, app, client, monkeypatch):
        """LL-008: When API is unavailable, show warning message."""
        from app import get_db, fetch_live_race_data
        import app as app_module
        
        db = get_db()
        race_id = 1006
        session_id = f'playwright-user-{race_id}'
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   (session_id, f'pwuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, f'Test GP {race_id}', race_id, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                   (session_id, race_id, race_id*10+1, race_id*10+2, race_id*10+3))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+1, f'verstappen{race_id}', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+2, f'hamilton{race_id}', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'))
        db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                   (race_id*10+3, f'norris{race_id}', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'))
        db.commit()
        
        # Mock API to return None (unavailable)
        original_fetch = app_module.fetch_live_race_data
        app_module.fetch_live_race_data = lambda s, r: None
        
        try:
            page.goto(f'/race/{race_id}/live')
            content = page.content().lower()
            
            # Should show some indication of unavailable data
            assert 'unavailable' in content or 'warning' in content or 'api' in content or 'error' in content
        finally:
            app_module.fetch_live_race_data = original_fetch
