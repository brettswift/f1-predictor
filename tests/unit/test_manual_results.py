"""Unit tests for manual race results entry (admin functionality)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestManualResultsEntry:
    """Test cases for admin manual results entry."""

    def test_enter_results_page_loads(self, app, client):
        """T-MR-001: Admin can access enter-results page for a race.
        
        Given an admin is logged in
        And a race exists with status 'locked'
        When the admin visits /admin/enter-results/<race_id>
        Then the page should load with driver selection form
        """
        from app import get_db
        db = get_db()
        
        race_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (991, 'Test Grand Prix', 991, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (991, 'test_p1', 'Test Driver P1', 1, 'TP1'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (992, 'test_p2', 'Test Driver P2', 2, 'TP2'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (993, 'test_p3', 'Test Driver P3', 3, 'TP3'))
        db.commit()
        
        # Admin user setup - username 'brett' is in ADMIN_USERNAMES
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session-123'
        
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('admin-session-123', 'brett'))
        db.commit()
        
        response = client.get('/admin/enter-results/991')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

    def test_enter_results_success(self, app, client):
        """T-MR-002: Admin can submit manual results and scores are calculated.
        
        Given a race is locked with predictions
        When an admin submits P1/P2/P3 drivers
        Then results are stored in database
        And scores are calculated for existing predictions
        """
        from app import get_db
        db = get_db()
        
        race_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (992, 'Test Grand Prix 2', 992, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (994, 'winner', 'Winner Driver', 1, 'WIN'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (995, 'second', 'Second Driver', 2, 'SEC'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (996, 'third', 'Third Driver', 3, 'THR'))
        
        # Admin user
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('admin-session-456', 'brett'))
        
        # Regular user with prediction
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('user-session-789', 'testuser'))
        
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('user-session-789', 992, 994, 995, 996))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session-456'
        
        response = client.post('/admin/enter-results/992', data={
            'p1': '994',
            'p2': '995',
            'p3': '996'
        }, follow_redirects=True)
        
        assert response.status_code == 200
        
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (992,)).fetchone()
        assert result is not None, "Results should be stored"
        assert result['p1_driver_id'] == 994
        assert result['p2_driver_id'] == 995
        assert result['p3_driver_id'] == 996
        
        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('user-session-789', 992)).fetchone()
        assert score is not None, "Score should be calculated"
        assert score['points'] == 20, "Perfect prediction should score 20 points (10+6+4)"

    def test_enter_results_missing_fields(self, app, client):
        """T-MR-003: Admin cannot submit results with missing driver selections.
        
        Given an admin is on the enter-results page
        When the form is submitted without all three positions
        Then an error message should be shown
        """
        from app import get_db
        db = get_db()
        
        race_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (993, 'Test Grand Prix 3', 993, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (997, 'driver1', 'Driver One', 1, 'D01'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (998, 'driver2', 'Driver Two', 2, 'D02'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (999, 'driver3', 'Driver Three', 3, 'D03'))
        
        # Admin user
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('admin-session-333', 'brett'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session-333'
        
        response = client.post('/admin/enter-results/993', data={
            'p1': '997',
            'p2': '',
            'p3': '999'
        }, follow_redirects=True)
        
        assert response.status_code == 200
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (993,)).fetchone()
        assert result is None, "Results should not be stored with missing fields"

    def test_enter_results_updates_existing(self, app, client):
        """T-MR-004: Submitting results for race with existing results updates them.
        
        Given a race already has results entered
        When an admin submits new results
        Then the existing results should be updated (ON CONFLICT)
        """
        from app import get_db
        db = get_db()
        
        race_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (994, 'Test Grand Prix 4', 994, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1001, 'old_winner', 'Old Winner', 1, 'OLD'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1002, 'old_second', 'Old Second', 2, 'OLS'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1003, 'old_third', 'Old Third', 3, 'OLD'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1004, 'new_winner', 'New Winner', 1, 'NEW'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1005, 'new_second', 'New Second', 2, 'NWS'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1006, 'new_third', 'New Third', 3, 'NWT'))
        
        # Admin user
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('admin-session-444', 'brett'))
        
        # Existing results
        db.execute('''
            INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?)
        ''', (994, 1001, 1002, 1003))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session-444'
        
        response = client.post('/admin/enter-results/994', data={
            'p1': '1004',
            'p2': '1005',
            'p3': '1006'
        }, follow_redirects=True)
        
        assert response.status_code == 200
        
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (994,)).fetchone()
        assert result['p1_driver_id'] == 1004, "P1 should be updated to new winner"
        assert result['p2_driver_id'] == 1005, "P2 should be updated"
        assert result['p3_driver_id'] == 1006, "P3 should be updated"

    def test_enter_results_race_not_found(self, app, client):
        """T-MR-005: Entering results for non-existent race redirects with error.
        
        Given a race does not exist
        When an admin tries to access /admin/enter-results/<invalid_id>
        Then they should be redirected with a flash error message
        """
        # Admin user
        from app import get_db
        db = get_db()
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('admin-session-555', 'brett'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session-555'
        
        response = client.get('/admin/enter-results/99999')
        # App redirects to /races with flash message, not 404
        assert response.status_code == 302, f"Expected 302 redirect, got {response.status_code}"

    def test_non_admin_cannot_access(self, app, client):
        """T-MR-006: Non-admin users cannot access enter-results page.
        
        Given a non-admin user is logged in
        When they try to access /admin/enter-results/<race_id>
        Then they should be redirected (not authorized)
        """
        from app import get_db
        db = get_db()
        
        race_time = datetime(2026, 3, 23, 14, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (995, 'Test Grand Prix 5', 995, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1011, 'dp1', 'Driver P1', 1, 'DP1'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1012, 'dp2', 'Driver P2', 2, 'DP2'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (1013, 'dp3', 'Driver P3', 3, 'DP3'))
        
        # Non-admin user
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('nonadmin-session', 'notbrett'))
        db.commit()
        
        with client.session_transaction() as sess:
            sess['session_id'] = 'nonadmin-session'
        
        response = client.get('/admin/enter-results/995')
        assert response.status_code == 302, f"Non-admin should be redirected, got {response.status_code}"

    def test_unauthenticated_cannot_access(self, app, client):
        """T-MR-007: Unauthenticated users cannot access enter-results page.
        
        Given no user is logged in
        When they try to access /admin/enter-results/<race_id>
        Then they should be redirected to login
        """
        response = client.get('/admin/enter-results/1')
        assert response.status_code == 302, f"Unauthenticated should be redirected, got {response.status_code}"