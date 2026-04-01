"""Unit tests for prediction locking (T-RL-002, T-RL-005)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestPredictionLocking:
    """Test prediction acceptance/rejection based on race lock state."""

    def _insert_race_and_drivers(self, db, round_num, status='open', hours_ahead=24):
        """Helper: insert a race and 3 drivers, return actual race id."""
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
        for i, code in enumerate(['D01', 'D02', 'D03']):
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (base_id + i, f'driver_{base_id+i}', f'Driver {base_id+i}', base_id+i, code)
            )
        db.commit()
        return race_id

    def _login(self, client, username):
        """Helper: set username and return session."""
        client.post('/set-username', data={'username': username})

    def test_prediction_form_enabled_before_race_start(self, app, client):
        """T-RL-002a: Prediction form is enabled before race starts.
        
        Given a race is scheduled to start at 14:00 UTC tomorrow
        When a logged-in user visits /predict/<race_id>
        Then the form should be displayed (status 200)
        """
        from app import get_db
        db = get_db()
        race_id = self._insert_race_and_drivers(db, round_num=110, status='open', hours_ahead=24)

        self._login(client, 'testuser110')

        response = client.get(f'/predict/{race_id}')
        assert response.status_code == 200, \
            f"Expected 200 (form available), got {response.status_code}"

    def test_prediction_accepted_before_race_start(self, app, client):
        """T-RL-002b: Predictions are accepted before race starts.
        
        Given a race is scheduled to start at 14:00 UTC tomorrow
        When a logged-in user POSTs a prediction
        Then the prediction should be saved (redirect to /home)
        """
        from app import get_db
        db = get_db()
        race_id = self._insert_race_and_drivers(db, round_num=111, status='open', hours_ahead=24)

        self._login(client, 'testuser111')

        response = client.post(f'/predict/{race_id}', data={
            'p1': str(race_id),
            'p2': str(race_id + 1),
            'p3': str(race_id + 2)
        }, follow_redirects=False)

        assert response.status_code == 302, \
            f"Expected 302 (redirect after save), got {response.status_code}"

        prediction = db.execute(
            'SELECT * FROM predictions WHERE race_id = ?', (race_id,)
        ).fetchone()
        assert prediction is not None, "Prediction should be saved before race start"

    def test_prediction_form_rejected_after_lock(self, app, client):
        """T-RL-005a: Prediction form redirects after race is locked.
        
        Given a race has status='locked'
        When a logged-in user visits /predict/<race_id>
        Then the request should be redirected (302) to /home with error flash
        """
        from app import get_db
        db = get_db()
        race_id = self._insert_race_and_drivers(db, round_num=112, status='locked')

        self._login(client, 'testuser112')

        response = client.get(f'/predict/{race_id}', follow_redirects=False)
        assert response.status_code == 302, \
            f"Expected 302 (redirect when locked), got {response.status_code}"

    def test_prediction_post_rejected_after_lock(self, app, client):
        """T-RL-005b: POST prediction is rejected after race is locked.
        
        Given a race has status='locked'
        When a logged-in user POSTs a prediction
        Then the request should be redirected (302) back to /home
        """
        from app import get_db
        db = get_db()
        race_id = self._insert_race_and_drivers(db, round_num=113, status='locked')

        self._login(client, 'testuser113')

        response = client.post(f'/predict/{race_id}', data={
            'p1': str(race_id),
            'p2': str(race_id + 1),
            'p3': str(race_id + 2)
        }, follow_redirects=False)

        assert response.status_code == 302, \
            f"Expected 302 (rejected after lock), got {response.status_code}"
