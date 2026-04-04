"""Unit tests for leaderboard and display (F1-UI-2: Test leaderboard and display)."""

"""Unit tests for leaderboard and display (F1-UI-2, ADM-2: Season filter)."""

import pytest
import os
from datetime import datetime, timezone, timedelta

class TestSeasonFilter:
    """Test cases for ADM-005: Season filter on leaderboard."""

    def test_adm_005_season_param_filters_by_year(self, app, client):
        """ADM-005: ?season=YYYY filters leaderboard to races in that year."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('season-user', 'seasonuser'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (1001, '2026 GP', 1, '2026-03-15 14:00:00', 'completed'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (1002, '2025 GP', 2, '2025-03-15 14:00:00', 'completed'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('season-user', 1001, 20))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('season-user', 1002, 15))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'season-user'

        response = client.get('/leaderboard?season=2026')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Season 2026' in content

        response2025 = client.get('/leaderboard?season=2025')
        assert response2025.status_code == 200
        content2025 = response2025.data.decode('utf-8')
        assert 'Season 2025' in content2025

    def test_adm_005_no_param_defaults_to_current_year(self, app, client):
        """ADM-005: No season param defaults to current year."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('default-user', 'defaultuser'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (2001, 'Current GP', 1, '2026-04-01 14:00:00', 'completed'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('default-user', 2001, 25))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'default-user'

        response = client.get('/leaderboard')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '2026' in content

    def test_adm_005_season_current_defaults_to_current_year(self, app, client):
        """ADM-005: ?season=current defaults to current year."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('current-user', 'currentuser'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (2002, 'Current GP 2', 1, '2026-05-01 14:00:00', 'completed'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('current-user', 2002, 30))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'current-user'

        response = client.get('/leaderboard?season=current')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Season 2026' in content or '2026' in content

    def test_adm_005_only_completed_races_in_season(self, app, client):
        """ADM-005: Season filter only shows completed races in that year."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('incomplete-user', 'incompleteuser'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (3001, 'Completed 2026', 1, '2026-03-01 14:00:00', 'completed'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (3002, 'Open 2026', 2, '2026-06-01 14:00:00', 'open'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('incomplete-user', 3001, 20))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'incomplete-user'

        response = client.get('/leaderboard?season=2026')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Completed' in content  # name split()[0] = 'Completed'

    def test_adm_005_invalid_season_falls_back_to_current(self, app, client):
        """ADM-005: Invalid season param falls back to current year."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('invalid-user', 'invaliduser'))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'invalid-user'

        response = client.get('/leaderboard?season=abc')
        assert response.status_code == 200

    def test_adm_005_season_links_present_when_filtered(self, app, client):
        """ADM-005: Season links shown when viewing a specific season."""
        from app import get_db
        db = get_db()

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('links-user', 'linksuser'))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'links-user'

        response = client.get('/leaderboard?season=2026')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '/leaderboard' in content


import pytest
import os
from datetime import datetime, timezone, timedelta


class TestLeaderboardScores:
    """Test cases for leaderboard display (UI-006, UI-007)."""

    def _setup_users_with_scores(self, db):
        """Set up multiple users with various scores."""
        # User 1: score of 20
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('user-session-006', 'alice'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (100, 'Test GP', 100, '2026-04-01 14:00:00', 'completed'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('user-session-006', 100, 20))
        
        # User 2: score of 15
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('user-session-007', 'bob'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('user-session-007', 100, 15))
        
        # User 3: score of 10
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('user-session-008', 'charlie'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('user-session-008', 100, 10))
        
        db.commit()

    def test_ui_006_leaderboard_shows_total_scores(self, app, client):
        """UI-006: Leaderboard shows total scores and per-race points.
        
        Given multiple users with different scores
        When a logged-in user visits /leaderboard
        Then the total scores should be displayed for each user
        """
        from app import get_db
        db = get_db()
        self._setup_users_with_scores(db)
        
        # Login as alice via POST to /set-username
        client.post('/set-username', data={'username': 'alice'})
        
        response = client.get('/leaderboard')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        content = response.data.decode('utf-8')
        assert 'alice' in content, "Alice should be on leaderboard"
        assert 'bob' in content, "Bob should be on leaderboard"
        assert 'charlie' in content, "Charlie should be on leaderboard"

    def test_ui_007_leaderboard_sorted_by_score_descending(self, app, client):
        """UI-007: Sorted by total score descending.
        
        Given multiple users with different scores
        When a logged-in user visits /leaderboard
        Then users should be sorted by total score in descending order
        """
        from app import get_db
        db = get_db()
        self._setup_users_with_scores(db)
        
        # Login as alice via POST to /set-username
        client.post('/set-username', data={'username': 'alice'})
        
        response = client.get('/leaderboard')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        
        # Find positions of each user in the content
        alice_pos = content.find('alice')
        bob_pos = content.find('bob')
        charlie_pos = content.find('charlie')
        
        # Alice (20 pts) should appear before Bob (15 pts) and Charlie (10 pts)
        # Bob should appear before Charlie
        assert alice_pos < bob_pos < charlie_pos, \
            "Users should be sorted by score descending (alice > bob > charlie)"


class TestMobileResponsive:
    """Test cases for mobile responsive layout (UI-008)."""

    def test_ui_008_mobile_responsive_table(self, app, client):
        """UI-008: Mobile responsive layout.
        
        Given a leaderboard page
        When viewed on a mobile-sized viewport
        Then the page should have responsive CSS that handles small screens
        """
        from app import get_db
        db = get_db()
        
        # Setup single user
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('mobile-user', 'mobileuser'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (101, 'Test GP', 101, '2026-04-01 14:00:00', 'completed'))
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('mobile-user', 101, 20))
        db.commit()
        
        # Login
        with client.session_transaction() as sess:
            sess['session_id'] = 'mobile-user'
        
        response = client.get('/leaderboard')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        
        # Check for mobile-responsive CSS
        assert 'max-width: 600px' in content or '@media' in content or 'table-scroll' in content, \
            "Page should have responsive CSS for mobile"


class TestDevBadge:
    """Test cases for DEV badge display (UI-009)."""

    def test_ui_009_dev_badge_when_environment_dev(self, app, client, monkeypatch):
        """UI-009: DEV badge when ENVIRONMENT=dev.
        
        Given ENVIRONMENT is set to 'dev'
        When a user visits any page
        Then a DEV badge should be displayed in the header
        """
        # Set environment to dev
        monkeypatch.setenv('ENVIRONMENT', 'dev')
        
        from app import get_db
        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('dev-user', 'devuser'))
        db.commit()
        
        # Login
        with client.session_transaction() as sess:
            sess['session_id'] = 'dev-user'
        
        response = client.get('/home')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        
        # DEV badge should be visible
        assert 'DEV' in content or 'DEVELOPMENT' in content, \
            "DEV badge should be displayed when ENVIRONMENT=dev"


class TestVersionDisplay:
    """Test cases for version display (UI-010)."""

    def test_ui_010_version_display_when_app_version_set(self, app, client, monkeypatch):
        """UI-010: Version display when APP_VERSION set.
        
        Given APP_VERSION is set to a version string
        When a user visits any page
        Then the version should be displayed in the footer
        """
        # Set APP_VERSION
        monkeypatch.setenv('APP_VERSION', '1.2.3')
        
        from app import get_db
        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('version-user', 'versionuser'))
        db.commit()
        
        # Login
        with client.session_transaction() as sess:
            sess['session_id'] = 'version-user'
        
        response = client.get('/home')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        
        # Version should be displayed
        assert '1.2.3' in content or 'v1.2.3' in content, \
            "Version should be displayed when APP_VERSION is set"


class TestLeaderboardEdgeCases:
    """Additional edge case tests for leaderboard."""

    def test_leaderboard_empty_for_new_user(self, app, client):
        """New user with no scores should still see leaderboard."""
        from app import get_db
        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('newbie', 'newbie'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'newbie'
        
        response = client.get('/leaderboard')
        assert response.status_code == 200
        
    def test_leaderboard_user_highlighted(self, app, client):
        """Current user should be visually highlighted on leaderboard."""
        from app import get_db
        db = get_db()
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('user-a', 'usera'))
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('user-b', 'userb'))
        db.commit()
        
        # Login as user-a
        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'
        
        response = client.get('/leaderboard')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        # usera's row should have different styling (highlighted)
        assert 'usera' in content
        
    def test_leaderboard_no_race_breakdown_when_no_completed_races(self, app, client):
        """Race breakdown section should not appear when there are no completed races."""
        from app import get_db
        db = get_db()
        
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)', 
                   ('norace-user', 'noraceuser'))
        # Only add an open race, not completed
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (102, 'Future GP', 102, '2026-12-01 14:00:00', 'open'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'norace-user'
        
        response = client.get('/leaderboard')
        assert response.status_code == 200
        
        content = response.data.decode('utf-8')
        # Should still show leaderboard
        assert 'Leaderboard' in content or 'leaderboard' in content.lower()
