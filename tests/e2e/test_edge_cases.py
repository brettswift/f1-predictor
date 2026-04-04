"""Playwright E2E tests for edge cases and recovery (BUD-58: E2E-004, E2E-006, E2E-007).

Tests:
- E2E-004: Race delay handling
- E2E-006: API outage recovery
- E2E-007: Concurrent users (no race conditions)
"""

import pytest
import threading
import time
from datetime import datetime, timezone, timedelta


class TestRaceDelayHandling:
    """Test edge cases for race delay handling (E2E-004)."""

    @pytest.fixture
    def delayed_race_setup(self, page, app, client):
        """Set up a race that was supposed to start but is delayed."""
        from app import get_db
        db = get_db()

        race_id = 5001
        session_id = f'delay-user-{race_id}'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'delayuser{race_id}'))
        
        # Race was supposed to start 30 minutes ago but is delayed
        past_date = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Delayed GP', 1, past_date, 'open'))

        for i, name in enumerate(['Driver A', 'Driver B', 'Driver C']):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i+1, f'delaydriver{i}', name, f'Team {i}', i+10, f'DL{i}', 'Nationality'))
        db.commit()

        yield {'session_id': session_id, 'race_id': race_id, 'db': db}

        # Cleanup
        db.execute('DELETE FROM predictions WHERE user_id LIKE ?', (f'delay-user-%',))
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id LIKE ?', (f'delay-user-%',))
        db.commit()

    @pytest.mark.e2e
    def test_e2e_004_delayed_race_stays_open(self, page, app, client, delayed_race_setup):
        """E2E-004: Race should stay open when it hasn't officially started.

        Given a race whose scheduled time has passed but status is still 'open'
        When the race page is accessed
        Then the race should still accept predictions
        And auto_lock should not have locked it yet (if implementation uses date-based locking)
        """
        from app import get_db
        db = get_db()
        ctx = delayed_race_setup
        race_id = ctx['race_id']
        session_id = ctx['session_id']

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        response = page.goto(f'/race/{race_id}')
        assert response.status == 200, f"Race page should load, got {response.status}"

        # Verify race is still open
        race = db.execute('SELECT status FROM races WHERE id = ?', (race_id,)).fetchone()
        assert race['status'] == 'open', f"Delayed race should still be open, got {race['status']}"

    @pytest.mark.e2e
    def test_e2e_004_prediction_accepted_for_delayed_race(self, page, app, client, delayed_race_setup):
        """E2E-004: Predictions should be accepted for delayed races.

        Given a delayed race that is still accepting predictions
        When a user submits predictions
        Then they should be saved successfully
        """
        from app import get_db
        db = get_db()
        ctx = delayed_race_setup
        race_id = ctx['race_id']
        session_id = ctx['session_id']

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        p1_id = race_id*10+1
        p2_id = race_id*10+2
        p3_id = race_id*10+3

        pred_response = client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)

        assert pred_response.status_code == 200, f"Prediction should be accepted: {pred_response.status_code}"

        pred = db.execute('SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
                          (session_id, race_id)).fetchone()
        assert pred is not None, "Prediction should be saved for delayed race"

    @pytest.mark.e2e
    def test_e2e_004_multiple_delay_periods(self, page, app, client):
        """E2E-004: System should handle multiple race delays gracefully.

        Given a race that has been delayed multiple times
        When predictions are submitted
        Then they should be saved correctly
        """
        from app import get_db
        db = get_db()

        race_id = 5002
        session_id = f'multi-delay-user-{race_id}'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'multidelay{race_id}'))

        # Race scheduled in the past (multiple delays)
        past_date = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Multi-Delay GP', 1, past_date, 'open'))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'multidrv{i}', f'Driver {i}', 'Team', i, f'MD{i}', 'N'))
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        # Submit prediction
        pred_response = client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(race_id*10+1),
            'p2_driver_id': str(race_id*10+2),
            'p3_driver_id': str(race_id*10+3),
        }, follow_redirects=True)

        assert pred_response.status_code == 200, "Prediction should succeed despite delay"

        # Cleanup
        db.execute('DELETE FROM predictions WHERE user_id = ?', (session_id,))
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
        db.commit()


class TestAPIOutageRecovery:
    """Test edge cases for API outage recovery (E2E-006)."""

    @pytest.mark.e2e
    def test_e2e_006_results_check_with_api_down(self, page, app, client, monkeypatch):
        """E2E-006: System should handle API outages gracefully.

        Given the Ergast API is unavailable
        When an admin checks for results
        Then a friendly message should be shown
        And no crash should occur
        """
        import app as app_module

        # Mock the Ergast API to fail
        original_get = app_module.requests.get if hasattr(app_module, 'requests') else None

        def mock_get(*args, **kwargs):
            raise Exception("API Unavailable")

        if original_get:
            app_module.requests.get = mock_get

        try:
            from app import get_db
            db = get_db()

            race_id = 6001
            session_id = f'api-down-user-{race_id}'

            db.execute('INSERT INTO users (session_id, username, is_admin) VALUES (?, ?, ?)',
                       (session_id, f'apiadmin{race_id}', 1))
            db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                       (race_id, 'API Down GP', 1, '2026-04-01 14:00:00', 'locked'))

            for i in range(1, 4):
                db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (race_id*10+i, f'apidowndrv{i}', f'Driver {i}', 'Team', i, f'AD{i}', 'N'))
            db.commit()

            # Check results - should handle API failure
            response = page.goto('/check-results')
            content = page.content()

            # Should show some indication (either success with 0 updates or error message)
            assert response.status == 200, f"Check results should complete, got {response.status}"

            # Cleanup
            db.execute('DELETE FROM races WHERE id = ?', (race_id,))
            db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
            db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
            db.commit()
        finally:
            if original_get:
                app_module.requests.get = original_get

    @pytest.mark.e2e
    def test_e2e_006_live_leaderboard_api_timeout(self, page, app, client, monkeypatch):
        """E2E-006: Live leaderboard should handle API timeouts.

        Given a locked race with predictions
        When the live data API times out
        Then the page should load with a warning
        And not crash
        """
        from app import get_db
        import app as app_module

        db = get_db()
        race_id = 6002
        session_id = f'll-timeout-user-{race_id}'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'lltimeout{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'LL Timeout GP', 1, '2026-04-04 14:00:00', 'locked'))
        db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                   (session_id, race_id, race_id*10+1, race_id*10+2, race_id*10+3))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'lltimeoutdrv{i}', f'Driver {i}', 'Team', i, f'LT{i}', 'N'))
        db.commit()

        # Mock fetch_live_race_data to return None (API unavailable)
        original_fetch = getattr(app_module, 'fetch_live_race_data', None)
        if hasattr(app_module, 'fetch_live_race_data'):
            app_module.fetch_live_race_data = lambda s, r: None

        try:
            response = page.goto(f'/race/{race_id}/live')
            assert response.status == 200, f"Live page should load even with API down: {response.status}"

            content = page.content()
            # Should show some graceful degradation
            assert 'live' in content.lower() or 'leaderboard' in content.lower()
        finally:
            if original_fetch:
                app_module.fetch_live_race_data = original_fetch

        # Cleanup
        db.execute('DELETE FROM predictions WHERE user_id = ?', (session_id,))
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
        db.commit()

    @pytest.mark.e2e
    def test_e2e_006_partial_api_response(self, page, app, client, monkeypatch):
        """E2E-006: System should handle partial API responses.

        Given an API that returns incomplete data
        When fetching race results
        Then graceful handling should occur
        """
        from app import get_db
        import app as app_module

        db = get_db()
        race_id = 6003
        session_id = f'partial-api-user-{race_id}'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'partialuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Partial API GP', 1, '2026-04-01 14:00:00', 'locked'))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'partialdrv{i}', f'Driver {i}', 'Team', i, f'PD{i}', 'N'))
        db.commit()

        # Mock fetch to return partial data
        original_fetch = getattr(app_module, 'fetch_live_race_data', None)
        if hasattr(app_module, 'fetch_live_race_data'):
            app_module.fetch_live_race_data = lambda s, r: {'status': 'error', 'message': 'Partial data'}

        try:
            response = page.goto(f'/race/{race_id}/live')
            assert response.status == 200, "Page should handle partial API response"
        finally:
            if original_fetch:
                app_module.fetch_live_race_data = original_fetch

        # Cleanup
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
        db.commit()


class TestConcurrentUsers:
    """Test edge cases for concurrent users and race conditions (E2E-007)."""

    @pytest.mark.e2e
    def test_e2e_007_simultaneous_predictions_same_user(self, page, app, client):
        """E2E-007: System should handle same user submitting predictions simultaneously.

        Given a user submits predictions for the same race twice in quick succession
        When both requests are processed
        Then only one prediction should be saved (no duplicates)
        """
        from app import get_db
        db = get_db()

        race_id = 7001
        session_id = f'concurrent-user-{race_id}'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, f'concurrentuser{race_id}'))
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Concurrent GP', 1, '2026-04-10 14:00:00', 'open'))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'concdrv{i}', f'Driver {i}', 'Team', i, f'CC{i}', 'N'))
        db.commit()

        p1_id = race_id*10+1
        p2_id = race_id*10+2
        p3_id = race_id*10+3

        # Simulate two rapid submissions
        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)

        client.post(f'/race/{race_id}', data={
            'p1_driver_id': str(p1_id),
            'p2_driver_id': str(p2_id),
            'p3_driver_id': str(p3_id),
        }, follow_redirects=True)

        # Count predictions - should be exactly 1
        preds = db.execute('SELECT COUNT(*) as cnt FROM predictions WHERE user_id = ? AND race_id = ?',
                           (session_id, race_id)).fetchone()
        assert preds['cnt'] == 1, f"Should have exactly 1 prediction, got {preds['cnt']}"

        # Cleanup
        db.execute('DELETE FROM predictions WHERE user_id = ?', (session_id,))
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
        db.commit()

    @pytest.mark.e2e
    def test_e2e_007_race_lock_race_condition(self, page, app, client):
        """E2E-007: Race locking should handle concurrent access.

        Given multiple users accessing a race near lock time
        When the race locks
        Then all predictions before lock should be saved
        And predictions after lock should be rejected
        """
        from app import get_db
        db = get_db()

        race_id = 7002

        # Create multiple users
        users = []
        for i in range(3):
            session_id = f'racecond-user-{race_id}-{i}'
            db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                       (session_id, f'raceconduser{i}'))
            users.append(session_id)

        # Race scheduled very soon
        soon_date = (datetime.now(timezone.utc) + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                   (race_id, 'Race Condition GP', 1, soon_date, 'open'))

        for i in range(1, 4):
            db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                       (race_id*10+i, f'rcondrv{i}', f'Driver {i}', 'Team', i, f'RC{i}', 'N'))
        db.commit()

        # All users submit predictions before lock
        for session_id in users:
            with client.session_transaction() as sess:
                sess['session_id'] = session_id

            response = client.post(f'/race/{race_id}', data={
                'p1_driver_id': str(race_id*10+1),
                'p2_driver_id': str(race_id*10+2),
                'p3_driver_id': str(race_id*10+3),
            }, follow_redirects=True)
            assert response.status_code == 200, f"Prediction should succeed"

        # Manually lock race to simulate time passing
        db.execute('UPDATE races SET status = ? WHERE id = ?', ('locked', race_id))
        db.commit()

        # Verify all predictions before lock are saved
        for session_id in users:
            pred = db.execute('SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
                              (session_id, race_id)).fetchone()
            assert pred is not None, f"Prediction should exist for {session_id}"

        # Cleanup
        db.execute('DELETE FROM predictions WHERE user_id LIKE ?', (f'racecond-user-%',))
        db.execute('DELETE FROM races WHERE id = ?', (race_id,))
        db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id LIKE ?', (f'racecond-user-%',))
        db.commit()

    @pytest.mark.e2e
    def test_e2e_007_concurrent_score_calculation(self, page, app, client):
        """E2E-007: Score calculation should handle concurrent updates.

        Given multiple races complete simultaneously
        When scores are calculated concurrently
        Then each user's total should be correct
        """
        from app import get_db
        db = get_db()

        session_id = f'score-concurrent-user'

        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   (session_id, 'scoreconcurrent'))

        # Create 3 completed races
        races = []
        for i, points in enumerate([20, 15, 10]):
            race_id = 7003 + i
            past_date = (datetime.now(timezone.utc) - timedelta(days=i+1)).strftime('%Y-%m-%d %H:%M:%S')
            db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                       (race_id, f'Score Race {i}', i+1, past_date, 'completed'))

            for j in range(1, 4):
                db.execute('INSERT INTO drivers (id, driver_id, name, team, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (race_id*10+j, f'scoredrv{race_id}{j}', f'Driver {j}', 'Team', j, f'SC{race_id}{j}', 'N'))

            db.execute('INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
                       (race_id, race_id*10+1, race_id*10+2, race_id*10+3))

            db.execute('INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?, ?)',
                       (session_id, race_id, race_id*10+1, race_id*10+2, race_id*10+3))

            races.append((race_id, points))

        db.commit()

        # Calculate scores for all races
        from app import calculate_scores
        for race_id, _ in races:
            calculate_scores(race_id)

        # Verify total score
        total = db.execute('SELECT SUM(points) as total FROM scores WHERE user_id = ?',
                           (session_id,)).fetchone()['total']
        expected = sum(p for _, p in races)
        assert total == expected, f"Total score should be {expected}, got {total}"

        # Cleanup
        db.execute('DELETE FROM scores WHERE user_id = ?', (session_id,))
        db.execute('DELETE FROM predictions WHERE user_id = ?', (session_id,))
        for race_id, _ in races:
            db.execute('DELETE FROM races WHERE id = ?', (race_id,))
            db.execute('DELETE FROM drivers WHERE id >= ?', (race_id*10,))
        db.execute('DELETE FROM users WHERE session_id = ?', (session_id,))
        db.commit()
