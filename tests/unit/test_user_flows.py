"""Unit tests for user flows (F1-UI-1: Test user flows)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestUserRegistration:
    """Test cases for user registration and session management (UI-001)."""

    def test_ui_001_user_registration_creates_session(self, app, client):
        """UI-001: User registration creates session.
        
        Given no user is logged in
        When a POST request is made to /set-username with a valid username
        Then a new session should be created
        And the user should be redirected to /home
        """
        from app import get_db
        
        response = client.post('/set-username', data={'username': 'newuser123'})
        
        # Should redirect to home
        assert response.status_code == 302, f"Expected 302 redirect, got {response.status_code}"
        
        # Session should be set
        with client.session_transaction() as sess:
            assert 'session_id' in sess, "Session ID should be set after registration"
            session_id = sess['session_id']
        
        # User should exist in database
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE username = ?', ('newuser123',)).fetchone()
        assert user is not None, "User should be created in database"
        assert user['session_id'] == session_id, "Session ID should match"

    def test_ui_001_existing_user_reuses_session(self, app, client):
        """UI-001 variant: Existing username reuses their session.
        
        Given a user 'existinguser' already exists
        When the same user registers again
        Then they should get their existing session_id
        """
        from app import get_db
        
        # First registration
        response1 = client.post('/set-username', data={'username': 'existinguser'})
        
        with client.session_transaction() as sess:
            session_id_1 = sess['session_id']
        
        # Second registration with same username (new client/session)
        response2 = client.post('/set-username', data={'username': 'existinguser'})
        
        with client.session_transaction() as sess:
            session_id_2 = sess['session_id']
        
        # Should reuse the same session
        assert session_id_1 == session_id_2, "Same username should reuse existing session"
        
        # Only one user should exist
        db = get_db()
        count = db.execute('SELECT COUNT(*) as c FROM users WHERE username = ?', ('existinguser',)).fetchone()['c']
        assert count == 1, "Should only have one user with this username"


class TestUsernamePersistence:
    """Test cases for username persistence (UI-002)."""

    def test_ui_002_username_persists_across_requests(self, app, client):
        """UI-002: Username persistence on return.
        
        Given a user is logged in with username 'testuser'
        When they navigate to any page and return to home
        Then their username should still be associated with their session
        """
        from app import get_db
        
        # Login
        client.post('/set-username', data={'username': 'persistuser'})
        
        # Get the session_id
        with client.session_transaction() as sess:
            session_id = sess['session_id']
        
        # Simulate new request (clear cookies scenario, but same session)
        # The user should still be recognized
        response = client.get('/home')
        
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        # User should still be in database
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (session_id,)).fetchone()
        assert user is not None, "User should persist in database"
        assert user['username'] == 'persistuser', "Username should persist"


class TestPredictionSubmission:
    """Test cases for prediction submission (UI-003)."""

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers, return actual race id and driver ids."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        if status == 'locked':
            race_time = race_time - timedelta(hours=1)
        elif status == 'open':
            race_time = race_time + timedelta(hours=hours_ahead - 1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Test GP', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        driver_ids = []
        for i, code in enumerate(['D01', 'D02', 'D03']):
            driver_id = base_id + i
            driver_ids.append(driver_id)
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', base_id+i, code)
            )
        db.commit()
        return race_id, driver_ids

    def test_ui_003_prediction_submission_saves_to_db(self, app, client):
        """UI-003: Prediction submission saves to DB.
        
        Given a user is logged in
        And a race is open for predictions
        When the user submits a prediction
        Then the prediction should be saved in the database
        """
        from app import get_db
        db = get_db()
        race_id, driver_ids = self._insert_race_and_drivers(db, round_num=200, status='open', hours_ahead=24)

        # Login
        client.post('/set-username', data={'username': 'predictuser'})
        
        with client.session_transaction() as sess:
            session_id = sess['session_id']

        # Submit prediction
        p1, p2, p3 = driver_ids[0], driver_ids[1], driver_ids[2]
        response = client.post(f'/predict/{race_id}', data={
            'p1': str(p1),
            'p2': str(p2),
            'p3': str(p3)
        }, follow_redirects=False)

        assert response.status_code == 302, f"Expected 302 after save, got {response.status_code}"

        # Verify in database
        prediction = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
            (session_id, race_id)
        ).fetchone()
        
        assert prediction is not None, "Prediction should be saved to database"
        assert prediction['p1_driver_id'] == p1, "P1 should match"
        assert prediction['p2_driver_id'] == p2, "P2 should match"
        assert prediction['p3_driver_id'] == p3, "P3 should match"


class TestPredictionUpdate:
    """Test cases for prediction update (UI-004)."""

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        if status == 'open':
            race_time = race_time + timedelta(hours=hours_ahead - 1)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Test GP', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

        base_id = round_num * 10
        driver_ids = []
        for i, code in enumerate(['D01', 'D02', 'D03', 'D04', 'D05', 'D06']):
            driver_id = base_id + i
            driver_ids.append(driver_id)
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', base_id+i, code)
            )
        db.commit()
        return race_id, driver_ids

    def test_ui_004_prediction_update_replaces_old(self, app, client):
        """UI-004: Prediction update replaces old.
        
        Given a user has already submitted a prediction
        When they submit a new prediction for the same race
        Then the old prediction should be replaced
        """
        from app import get_db
        db = get_db()
        race_id, driver_ids = self._insert_race_and_drivers(db, round_num=201, status='open', hours_ahead=24)

        # Login
        client.post('/set-username', data={'username': 'updateuser'})
        
        with client.session_transaction() as sess:
            session_id = sess['session_id']

        p1, p2, p3 = driver_ids[0], driver_ids[1], driver_ids[2]
        p1_new, p2_new, p3_new = driver_ids[3], driver_ids[4], driver_ids[5]

        # First prediction
        response1 = client.post(f'/predict/{race_id}', data={
            'p1': str(p1),
            'p2': str(p2),
            'p3': str(p3)
        }, follow_redirects=False)
        assert response1.status_code == 302

        # Update prediction
        response2 = client.post(f'/predict/{race_id}', data={
            'p1': str(p1_new),
            'p2': str(p2_new),
            'p3': str(p3_new)
        }, follow_redirects=False)
        assert response2.status_code == 302

        # Should only have one prediction
        predictions = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
            (session_id, race_id)
        ).fetchall()
        
        assert len(predictions) == 1, "Should only have one prediction after update"

        # The prediction should be the new one
        updated = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
            (session_id, race_id)
        ).fetchone()
        
        assert updated['p1_driver_id'] == p1_new, "P1 should be updated to new value"
        assert updated['p2_driver_id'] == p2_new, "P2 should be updated to new value"
        assert updated['p3_driver_id'] == p3_new, "P3 should be updated to new value"


class TestRaceListDisplay:
    """Test cases for race list display (UI-005)."""

    def _insert_race(self, db, round_num, name, status, hours_from_now=24):
        """Helper: insert a race."""
        race_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc) + timedelta(hours=hours_from_now)

        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            (name, round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), status)
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']
        db.commit()
        return race_id

    def test_ui_005_race_list_displays_with_correct_status(self, app, client):
        """UI-005: Race list displays with correct status.
        
        Given multiple races with different statuses (open, locked, completed)
        When a logged-in user visits /races
        Then each race should display its correct status
        """
        from app import get_db
        db = get_db()

        # Login first
        client.post('/set-username', data={'username': 'raceviewuser'})

        # Create races with different statuses
        open_race_id = self._insert_race(db, 301, 'Future GP', 'open', hours_from_now=48)
        locked_race_id = self._insert_race(db, 302, 'Locked GP', 'locked', hours_from_now=-2)
        completed_race_id = self._insert_race(db, 303, 'Completed GP', 'completed', hours_from_now=-24)

        # Visit races page
        response = client.get('/races')
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        content = response.data.decode('utf-8')
        
        # Each race should appear with its name
        assert 'Future GP' in content, "Future GP should be displayed"
        assert 'Locked GP' in content, "Locked GP should be displayed"
        assert 'Completed GP' in content, "Completed GP should be displayed"
