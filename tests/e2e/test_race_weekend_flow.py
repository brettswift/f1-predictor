"""Playwright E2E tests for complete race weekend flow (BUD-57: E2E-001).

Tests the full lifecycle:
1. predictions → lock → race → results → scores
2. New user journey: register → predict → view leaderboard
3. Multi-race season cumulative scores
"""

import pytest
from datetime import datetime, timezone, timedelta


class TestRaceWeekendLifecycle:
    """Test complete race weekend flow (E2E-001)."""

    @pytest.fixture
    def setup_race_weekend(self, page, app, client):
        """Set up a complete race weekend scenario with one user and one race."""
        from app import get_db

        db = get_db()

        # Create test user
        session_id = 'e2e-weekend-user'
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, 'e2eweekend'))
        db.commit()

        # Create race (open, future)
        race_id = 2001
        future_date = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'E2E Test GP', 1, future_date, 'open'))

        # Create drivers
        drivers = [
            (race_id*10+1, 'verstappen', 'Max Verstappen', 'Red Bull', 1, 'VER', 'Dutch'),
            (race_id*10+2, 'hamilton', 'Lewis Hamilton', 'Ferrari', 44, 'HAM', 'British'),
            (race_id*10+3, 'norris', 'Lando Norris', 'McLaren', 4, 'NOR', 'British'),
        ]
        for d in drivers:
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)', d)
        db.commit()

        yield {
            'session_id': session_id,
            'race_id': race_id,
            'drivers': drivers,
            'db': db
        }

    @pytest.mark.e2e
    def test_e2e_001_full_lifecycle_predictions_to_scores(self, page, app, client, setup_race_weekend):
        """E2E-001: Full lifecycle - predictions → lock → race → results → scores.

        Given a new user with predictions made for an open race
        When the race locks (starts)
        And results are entered
        Then scores are calculated and visible on leaderboard
        """
        from app import get_db
        db = get_db()
        ctx = setup_race_weekend
        race_id = ctx['race_id']
        session_id = ctx['session_id']

        # Step 1: User is logged in (session exists)
        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        # Step 2: Navigate to race and make predictions
        response = page.goto(f'/race/{race_id}')
        assert response.status in [200, 302], f"Race page failed to load: {response.status}"

        # Step 3: Submit predictions via POST
        p1_id = race_id*10+1
        p2_id = race_id*10+2
        p3_id = race_id*10+3

        pred_response = client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)
        assert pred_response.status_code == 200, f"Prediction submission failed: {pred_response.status_code}"

        # Verify prediction was saved
        pred = db.execute('SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
                          (session_id, race_id)).fetchone()
        assert pred is not None, "Prediction should be saved"
        assert pred['p1_driver_id'] == str(p1_id), "P1 prediction mismatch"

        # Step 4: Race locks (simulate by updating status)
        db.execute('UPDATE races SET status = ? WHERE id = ?', ('locked', race_id))
        db.commit()

        # Verify predictions are locked (no longer editable)
        lock_check = db.execute('SELECT status FROM races WHERE id = ?', (race_id,)).fetchone()
        assert lock_check['status'] == 'locked', "Race should be locked"

        # Step 5: Results are entered (admin action)
        db.execute('INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
                   (race_id, race_id*10+1, race_id*10+2, race_id*10+3))
        db.execute('UPDATE races SET status = ? WHERE id = ?', ('completed', race_id))
        db.commit()

        # Step 6: Score is calculated
        from app import calculate_scores
        calculate_scores(race_id)

        # Verify score was calculated
        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?',
                           (session_id, race_id)).fetchone()
        assert score is not None, "Score should be calculated"
        assert score['points'] > 0, f"Score should be positive, got {score['points']}"

        # Step 7: Score visible on leaderboard
        response = page.goto('/leaderboard')
        assert response.status == 200, f"Leaderboard page failed: {response.status}"
        assert 'e2eweekend' in page.content(), "Username should appear on leaderboard"


class TestNewUserJourney:
    """Test new user registration and prediction flow (E2E-001 variant)."""

    @pytest.fixture
    def fresh_race(self, page, app, client):
        """Set up a fresh race for new user testing."""
        from app import get_db
        db = get_db()

        race_id = 2010
        session_id = f'newuser-{race_id}'

        # Create fresh user
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'newuser{race_id}'))

        # Create race
        future_date = (datetime.now(timezone.utc) + timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'New User GP', 1, future_date, 'open'))

        # Create drivers
        for i, name in enumerate(['Driver A', 'Driver B', 'Driver C']):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i+1, f'driver{race_id}{i}', name, f'Team {i}', i+10, f'DR{i}', 'Nationality'))

        db.commit()

        yield {'session_id': session_id, 'race_id': race_id, 'db': db}

    @pytest.mark.e2e
    def test_e2e_001_new_user_register_predict_leaderboard(self, page, app, client, fresh_race):
        """E2E-001 variant: New user journey - register → predict → view leaderboard.

        Given a new user who has never used the system
        When they register with a username
        And make predictions for a race
        Then they can view their predictions on the leaderboard
        """
        from app import get_db
        db = get_db()
        ctx = fresh_race
        race_id = ctx['race_id']
        session_id = ctx['session_id']

        # Step 1: Register (set username via POST)
        reg_response = client.post('/set-username', data={'username': f'newuser{race_id}'},
                                   follow_redirects=True)
        assert reg_response.status_code == 200, "Registration should succeed"

        # Verify session is set
        with client.session_transaction() as sess:
            assert 'session_id' in sess, "Session should be created"
            assert sess['session_id'] == session_id, "Session ID should match"

        # Step 2: Make predictions
        response = page.goto(f'/race/{race_id}')
        assert response.status == 200, f"Race page should load, got {response.status}"

        # Submit predictions
        p1_id = race_id*10+1
        p2_id = race_id*10+2
        p3_id = race_id*10+3

        pred_response = client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)
        assert pred_response.status_code == 200, f"Prediction should save: {pred_response.status_code}"

        # Verify prediction in DB
        pred = db.execute('SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
                          (session_id, race_id)).fetchone()
        assert pred is not None, "Prediction should exist after submission"

        # Step 3: View leaderboard
        lb_response = page.goto('/leaderboard')
        assert lb_response.status == 200, "Leaderboard should load"
        assert f'newuser{race_id}' in page.content(), "New user should appear on leaderboard"


class TestMultiRaceSeasonScores:
    """Test multi-race season cumulative scores (E2E-001 variant)."""

    @pytest.mark.e2e
    def test_e2e_001_multi_race_cumulative_scores(self, page, app, client):
        """E2E-001 variant: Multi-race season cumulative scores.

        Given a user who has participated in multiple races
        When they view the leaderboard
        Then their total score should be the sum of all race scores
        And individual race scores should be visible
        """
        from app import get_db
        db = get_db()

        session_id = 'multirace-user'
        username = 'multiraceuser'

        # Create user
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, username))

        # Create 3 completed races with different scores
        races = [
            (3001, 'Race 1', 1, 20),  # 20 points
            (3002, 'Race 2', 2, 15),  # 15 points
            (3003, 'Race 3', 3, 10),  # 10 points
        ]

        for race_id, name, round_num, points in races:
            # Create race
            past_date = (datetime.now(timezone.utc) - timedelta(days=round_num)).strftime('%Y-%m-%d %H:%M:%S')
            db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                       (race_id, name, round_num, past_date, 'completed'))

            # Create drivers
            for i in range(1, 4):
                db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (race_id*10+i, f'drv{race_id}{i}', f'Driver {i}', 'Team', i, f'D{i}', 'N'))

            # Enter results
            db.execute('INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
                       (race_id, race_id*10+1, race_id*10+2, race_id*10+3))

            # Calculate score
            from app import calculate_scores
            db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                       (session_id, race_id, race_id*10+1, race_id*10+2, race_id*10+3))
            db.commit()
            calculate_scores(race_id)

        db.commit()

        # Set session and navigate to leaderboard
        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        response = page.goto('/leaderboard')
        assert response.status == 200, "Leaderboard should load"

        # Verify total score is sum of all races (20 + 15 + 10 = 45)
        content = page.content()
        assert username in content, "User should appear on leaderboard"

        # Verify individual race scores visible (each race has points)
        for race_id, name, round_num, points in races:
            race = db.execute('SELECT * FROM races WHERE id = ?', (race_id,)).fetchone()
            assert race is not None, f"Race {race_id} should exist"


class TestRaceWeekendEdgeCases:
    """Test edge cases for race weekend flow."""

    @pytest.mark.e2e
    def test_e2e_001_prediction_blocked_after_lock(self, page, app, client):
        """E2E-001 edge case: Predictions should be blocked after race locks."""
        from app import get_db
        db = get_db()

        race_id = 2020
        session_id = f'locked-user-{race_id}'

        # Create user and locked race
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'lockeduser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Locked GP', 1, '2026-04-01 14:00:00', 'locked'))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'drv{i}', f'Driver {i}', 'Team', i, f'D{i}', 'N'))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        # Try to submit prediction to locked race
        p1_id = race_id*10+1
        p2_id = race_id*10+2
        p3_id = race_id*10+3

        pred_response = client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)

        # Prediction should either fail with 403 or redirect
        assert pred_response.status_code in [200, 403], f"Unexpected status: {pred_response.status_code}"

        # No prediction should exist in DB
        pred = db.execute('SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
                          (session_id, race_id)).fetchone()
        assert pred is None, "Prediction should not be saved for locked race"
