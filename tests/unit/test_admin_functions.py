"""Unit tests for admin functions (F1-ADM-1: Test admin functions).

T-IDs: ADM-001 to ADM-007

ACs:
- Admin can lock race
- Admin can enter results
- Admin can delete predictions
- Non-admin blocked (403)
- Auth required (redirect to login)
"""

import pytest
from datetime import datetime, timezone, timedelta


class TestAdminAuth:
    """Test admin authentication and authorization."""

    def _login(self, client, username):
        """Helper: set username and return session."""
        client.post('/set-username', data={'username': username})

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers, return race dict."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        if status == 'open':
            race_time = race_time + timedelta(hours=hours_ahead - 1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        drivers = []
        for i, code in enumerate(['D01', 'D02', 'D03', 'D04', 'D05']):
            driver_id = base_id + i
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', driver_id, code)
            )
            drivers.append({'id': driver_id, 'code': code})
        db.commit()
        return {'id': race_id, 'round': round_num, 'drivers': drivers}

    def test_adm_005_auth_required_redirects_to_login(self, app, client):
        """ADM-005: Auth required - unauthenticated users redirected to login.
        
        Given no user is logged in
        When a request is made to an admin endpoint
        Then the user should be redirected to the login page
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=201, status='open', hours_ahead=24)

        # Try to access admin lock race without login
        response = client.get(f'/admin/lock-race/{race["id"]}', follow_redirects=False)
        
        # Should redirect to index (login page) not proceed to admin
        assert response.status_code == 302, \
            f"Expected 302 redirect for unauthenticated user, got {response.status_code}"

    def test_adm_004_non_admin_blocked_with_flash(self, app, client):
        """ADM-004: Non-admin users are blocked from admin functions.
        
        Given a non-admin user 'regularuser' is logged in
        When they try to access /admin/lock-race/<id>
        Then they should be blocked with a flash message
        And redirected away from admin page
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=202, status='open', hours_ahead=24)

        self._login(client, 'regularuser')

        response = client.get(f'/admin/lock-race/{race["id"]}', follow_redirects=False)
        
        assert response.status_code == 302, \
            f"Expected 302 redirect for non-admin user, got {response.status_code}"

    def test_adm_004_non_admin_cannot_post_results(self, app, client):
        """ADM-004 variant: Non-admin cannot POST to enter-results.
        
        Given a non-admin user is logged in
        When they POST to /admin/enter-results/<id>
        Then they should be blocked (302 redirect)
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=203, status='open', hours_ahead=24)

        self._login(client, 'notadmin')

        response = client.post(f'/admin/enter-results/{race["id"]}', 
                               data={'p1': race['drivers'][0]['id'],
                                     'p2': race['drivers'][1]['id'],
                                     'p3': race['drivers'][2]['id']},
                               follow_redirects=False)
        
        assert response.status_code == 302, \
            f"Expected 302 redirect for non-admin POST, got {response.status_code}"


class TestAdminLockRace:
    """Test admin race locking functionality (ADM-001)."""

    def _login(self, client, username):
        """Helper: set username."""
        client.post('/set-username', data={'username': username})

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers, return race dict."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        if status == 'open':
            race_time = race_time + timedelta(hours=hours_ahead - 1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        drivers = []
        for i, code in enumerate(['D01', 'D02', 'D03']):
            driver_id = base_id + i
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', driver_id, code)
            )
            drivers.append({'id': driver_id, 'code': code})
        db.commit()
        return {'id': race_id, 'round': round_num, 'drivers': drivers}

    def test_adm_001_admin_can_lock_race(self, app, client):
        """ADM-001: Admin can lock a race.
        
        Given 'brett' (admin) is logged in
        And a race with status='open' exists
        When admin visits /admin/lock-race/<id>
        Then the race status should change to 'locked'
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=301, status='open', hours_ahead=24)

        # Verify initial status is open
        initial_race = db.execute('SELECT status FROM races WHERE id = ?', (race['id'],)).fetchone()
        assert initial_race['status'] == 'open', "Race should start as open"

        self._login(client, 'brett')

        response = client.get(f'/admin/lock-race/{race["id"]}', follow_redirects=True)
        assert response.status_code == 200, f"Expected 200 after lock, got {response.status_code}"

        # Verify status changed to locked
        updated_race = db.execute('SELECT status FROM races WHERE id = ?', (race['id'],)).fetchone()
        assert updated_race['status'] == 'locked', \
            f"Race should be locked after admin action, got {updated_race['status']}"

    def test_adm_001_lock_race_shows_flash_message(self, app, client):
        """ADM-001 variant: Admin lock shows success flash.
        
        Given admin is logged in
        When they lock a race
        Then a success flash message should appear
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=302, status='open', hours_ahead=24)

        self._login(client, 'brett')

        response = client.get(f'/admin/lock-race/{race["id"]}', follow_redirects=True)
        
        # Check flash message in response
        response_text = response.data.decode()
        assert 'Race predictions locked' in response_text or 'locked' in response_text.lower(), \
            "Success message should appear after locking race"


class TestAdminEnterResults:
    """Test admin results entry functionality (ADM-002)."""

    def _login(self, client, username):
        """Helper: set username."""
        client.post('/set-username', data={'username': username})

    def _insert_race_and_drivers(self, db, round_num, status='locked'):
        """Helper: insert a race and 5 drivers, return race dict."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc) - timedelta(hours=1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        drivers = []
        for i, code in enumerate(['D01', 'D02', 'D03', 'D04', 'D05']):
            driver_id = base_id + i
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', driver_id, code)
            )
            drivers.append({'id': driver_id, 'code': code})
        db.commit()
        return {'id': race_id, 'round': round_num, 'drivers': drivers}

    def _insert_user_and_prediction(self, db, username, race_id, p1, p2, p3):
        """Helper: insert user and prediction."""
        db.execute('INSERT INTO users (username, session_id) VALUES (?, ?)',
                   (username, f'session_{username}'))
        user = db.execute('SELECT id FROM users WHERE username = ?', (username,)).fetchone()
        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            (user['id'], race_id, p1, p2, p3)
        )
        db.commit()

    def test_adm_002_admin_can_enter_results(self, app, client):
        """ADM-002: Admin can enter race results.
        
        Given 'brett' (admin) is logged in
        And a locked race exists with no results
        When admin submits results via POST /admin/enter-results/<id>
        Then results should be saved to the database
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=401, status='locked')

        # Verify no results exist
        results_before = db.execute('SELECT 1 FROM results WHERE race_id = ?', (race['id'],)).fetchone()
        assert results_before is None, "No results should exist before admin entry"

        self._login(client, 'brett')

        p1, p2, p3 = race['drivers'][0]['id'], race['drivers'][1]['id'], race['drivers'][2]['id']
        response = client.post(f'/admin/enter-results/{race["id"]}',
                               data={'p1': p1, 'p2': p2, 'p3': p3},
                               follow_redirects=True)
        
        assert response.status_code == 200, f"Expected 200 after results entry, got {response.status_code}"

        # Verify results saved
        results_after = db.execute(
            'SELECT * FROM results WHERE race_id = ?', (race['id'],)
        ).fetchone()
        assert results_after is not None, "Results should be saved after admin entry"
        assert results_after['p1_driver_id'] == p1
        assert results_after['p2_driver_id'] == p2
        assert results_after['p3_driver_id'] == p3

    def test_adm_002_results_calculate_scores(self, app, client):
        """ADM-002 variant: Entering results calculates scores for predictions.
        
        Given a race with predictions exists
        When admin enters results
        Then scores should be calculated for all predictions
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=402, status='locked')

        # Add a user with prediction matching the drivers
        p1, p2, p3 = race['drivers'][0]['id'], race['drivers'][1]['id'], race['drivers'][2]['id']
        self._insert_user_and_prediction(db, 'predictor1', race['id'], p1, p2, p3)

        self._login(client, 'brett')

        response = client.post(f'/admin/enter-results/{race["id"]}',
                               data={'p1': p1, 'p2': p2, 'p3': p3},
                               follow_redirects=True)
        
        assert response.status_code == 200

        # Verify score was calculated (perfect prediction = 20 points)
        user = db.execute('SELECT id FROM users WHERE username = ?', ('predictor1',)).fetchone()
        score = db.execute('SELECT points FROM scores WHERE user_id = ? AND race_id = ?',
                          (user['id'], race['id'])).fetchone()
        assert score is not None, "Score should be calculated after results entry"
        assert score['points'] == 20, f"Perfect prediction should be 20 points, got {score['points']}"

    def test_adm_002_partial_results_update_existing(self, app, client):
        """ADM-002 variant: Admin can update existing results.
        
        Given a race already has results
        When admin enters new results
        Then the results should be updated (ON CONFLICT DO UPDATE)
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=403, status='locked')

        # Pre-enter results
        p1_old, p2_old, p3_old = race['drivers'][0]['id'], race['drivers'][1]['id'], race['drivers'][2]['id']
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
            (race['id'], p1_old, p2_old, p3_old)
        )
        db.commit()

        self._login(client, 'brett')

        # Enter new results with different drivers
        p1_new, p2_new, p3_new = race['drivers'][3]['id'], race['drivers'][4]['id'], race['drivers'][0]['id']
        response = client.post(f'/admin/enter-results/{race["id"]}',
                               data={'p1': p1_new, 'p2': p2_new, 'p3': p3_new},
                               follow_redirects=True)
        
        assert response.status_code == 200

        results = db.execute('SELECT * FROM results WHERE race_id = ?', (race['id'],)).fetchone()
        assert results['p1_driver_id'] == p1_new, "P1 should be updated"
        assert results['p2_driver_id'] == p2_new, "P2 should be updated"
        assert results['p3_driver_id'] == p3_new, "P3 should be updated"


class TestAdminDeletePredictions:
    """Test admin prediction deletion functionality (ADM-003)."""

    def _login(self, client, username):
        """Helper: set username."""
        client.post('/set-username', data={'username': username})

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        if status == 'open':
            race_time = race_time + timedelta(hours=hours_ahead - 1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        drivers = []
        for i, code in enumerate(['D01', 'D02', 'D03']):
            driver_id = base_id + i
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', driver_id, code)
            )
            drivers.append({'id': driver_id, 'code': code})
        db.commit()
        return {'id': race_id, 'round': round_num, 'drivers': drivers}

    def _insert_user(self, db, username):
        """Helper: insert user."""
        db.execute('INSERT INTO users (username, session_id) VALUES (?, ?)',
                   (username, f'session_{username}'))
        db.commit()
        return db.execute('SELECT id FROM users WHERE username = ?', (username,)).fetchone()

    def test_adm_003_admin_can_delete_predictions_get_page(self, app, client):
        """ADM-003: Admin can access delete-predictions page.
        
        Given 'brett' (admin) is logged in
        When admin visits /admin/delete-predictions (GET)
        Then the page should load successfully (200)
        """
        self._login(client, 'brett')

        response = client.get('/admin/delete-predictions')
        
        assert response.status_code == 200, \
            f"Admin should be able to access delete-predictions page, got {response.status_code}"

    def test_adm_003_admin_can_delete_matching_predictions(self, app, client):
        """ADM-003 variant: Admin can delete predictions matching criteria.
        
        Given a race with multiple user predictions exists
        And admin submits deletion for username pattern
        When predictions match the criteria
        Then those predictions should be deleted
        """
        from app import get_db
        db = get_db()
        race = self._insert_race_and_drivers(db, round_num=501, status='open', hours_ahead=24)

        # Create users and predictions
        user1 = self._insert_user(db, 'baduser')
        user2 = self._insert_user(db, 'gooduser')

        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            (user1['id'], race['id'], race['drivers'][0]['id'], race['drivers'][1]['id'], race['drivers'][2]['id'])
        )
        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
            (user2['id'], race['id'], race['drivers'][0]['id'], race['drivers'][1]['id'], race['drivers'][2]['id'])
        )
        db.commit()

        self._login(client, 'brett')

        # Submit deletion for 'baduser' predictions
        response = client.post('/admin/delete-predictions',
                               data={'username_pattern': 'baduser',
                                     'race_id': race['id'],
                                     'exclude_driver_id': race['drivers'][0]['id']},
                               follow_redirects=True)
        
        assert response.status_code == 200

        # Verify baduser's prediction was deleted
        pred_count = db.execute(
            'SELECT COUNT(*) as cnt FROM predictions WHERE user_id = ? AND race_id = ?',
            (user1['id'], race['id'])
        ).fetchone()['cnt']
        assert pred_count == 0, "Matching prediction should be deleted"

        # Verify gooduser's prediction remains
        pred_count_good = db.execute(
            'SELECT COUNT(*) as cnt FROM predictions WHERE user_id = ? AND race_id = ?',
            (user2['id'], race['id'])
        ).fetchone()['cnt']
        assert pred_count_good == 1, "Non-matching prediction should remain"


class TestAdminRouteProtection:
    """Additional tests for admin route protection (ADM-004, ADM-005, ADM-006, ADM-007)."""

    def _login(self, client, username):
        """Helper: set username."""
        client.post('/set-username', data={'username': username})

    def test_adm_006_non_admin_cannot_access_delete_predictions_post(self, app, client):
        """ADM-006: Non-admin cannot POST to delete-predictions.
        
        Given a non-admin user is logged in
        When they POST to /admin/delete-predictions
        Then they should be blocked (302 redirect)
        """
        self._login(client, 'regularuser')

        response = client.post('/admin/delete-predictions',
                               data={'username_pattern': 'test'},
                               follow_redirects=False)
        
        assert response.status_code == 302, \
            f"Non-admin should be blocked from POST, got {response.status_code}"

    def test_adm_007_different_admin_usernames_case_insensitive(self, app, client):
        """ADM-007: Admin check is case-insensitive.
        
        Given users 'Brett', 'BRETT', and 'brett' exist
        When any of them access admin functions
        Then they should all be granted admin access
        """
        from app import get_db
        db = get_db()

        # Insert race
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc) + timedelta(hours=12)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Test GP', 601, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (601,)).fetchone()['id']
        db.commit()

        for username in ['Brett', 'BRETT', 'brett']:
            # Login as this user
            client.post('/set-username', data={'username': username})

            # They should be able to access admin (not redirected)
            response = client.get(f'/admin/lock-race/{race_id}', follow_redirects=False)
            
            # If admin, should not be 302 (or if 302, should be to races, not index)
            if response.status_code == 302:
                # Check it didn't redirect to index (login page)
                location = response.headers.get('Location', '')
                assert 'index' not in location.lower(), \
                    f"User '{username}' should have admin access"
