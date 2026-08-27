"""Unit tests for magic-link email authentication (BUD-142 / F1-10)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestMagicLinkLogin:
    """Tests for the email magic-link authentication flow."""

    def test_login_page_renders_email_form(self, app, client):
        """AC: Users can enter an email on a new /login route."""
        response = client.get('/login')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'method="POST"' in content
        assert 'name="email"' in content
        assert '/login/request' in content

    def test_login_request_creates_token(self, app, client):
        """AC: Login request generates a token in the database."""
        from app import get_db

        response = client.post('/login/request', data={'email': 'test@example.com'})
        assert response.status_code == 200

        db = get_db()
        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('test@example.com',)
        ).fetchone()
        assert token is not None
        assert token['used'] == 0
        assert token['token'] is not None
        assert len(token['token']) > 0

    def test_login_request_normalizes_email_case(self, app, client):
        """Email addresses are normalized to lowercase for storage."""
        from app import get_db

        client.post('/login/request', data={'email': 'MixedCase@Example.COM'})
        db = get_db()
        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('mixedcase@example.com',)
        ).fetchone()
        assert token is not None

    def test_token_email_contains_correct_url(self, app, client):
        """AC: Token email contains the correct one-time login URL."""
        from app import get_db

        client.post('/login/request', data={'email': 'urltest@example.com'})
        db = get_db()
        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('urltest@example.com',)
        ).fetchone()

        assert token is not None

        # Debug endpoint returns the same token and a link containing it.
        response = client.get('/debug/magic-link/urltest@example.com')
        assert response.status_code == 200
        data = response.get_json()
        assert data['token'] == token['token']
        assert data['link'] == f'/login/verify/{token["token"]}'

    def test_token_expires(self, app, client, time_controller):
        """AC: Token expires after the TTL and cannot be used."""
        from app import get_db, consume_login_token

        client.post('/login/request', data={'email': 'expired@example.com'})
        db = get_db()
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('expired@example.com',)
        ).fetchone()
        token = row['token']

        # Jump past the 15-minute expiry window.
        time_controller.freeze(datetime.now(timezone.utc) + timedelta(minutes=16))

        email = consume_login_token(token)
        assert email is None

    def test_valid_token_logs_user_in(self, app, client):
        """AC: Visiting a valid magic link sets a secure session cookie
        and binds the email to that session."""
        from app import get_db

        # Request a magic link.
        client.post('/login/request', data={'email': 'valid@example.com'})
        db = get_db()
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('valid@example.com',)
        ).fetchone()
        token = row['token']

        # Visit the verification link.
        response = client.get(f'/login/verify/{token}', follow_redirects=False)
        assert response.status_code == 302
        assert response.location == '/home'

        # Session cookie should be set with a session_id.
        with client.session_transaction() as sess:
            assert 'session_id' in sess
            session_id = sess['session_id']

        # User row should have the email bound.
        user = db.execute(
            'SELECT * FROM users WHERE email = ?', ('valid@example.com',)
        ).fetchone()
        assert user is not None
        assert user['session_id'] == session_id

    def test_invalid_token_rejected(self, app, client):
        """AC: Invalid/reused token rejected."""
        response = client.get('/login/verify/not-a-real-token', follow_redirects=False)
        assert response.status_code == 302
        assert response.location == '/login'

    def test_reused_token_rejected(self, app, client):
        """AC: A token can only be used once."""
        from app import get_db

        client.post('/login/request', data={'email': 'reuse@example.com'})
        db = get_db()
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('reuse@example.com',)
        ).fetchone()
        token = row['token']

        # First use succeeds.
        response1 = client.get(f'/login/verify/{token}', follow_redirects=False)
        assert response1.status_code == 302
        assert response1.location == '/home'

        # Second use fails.
        response2 = client.get(f'/login/verify/{token}', follow_redirects=False)
        assert response2.status_code == 302
        assert response2.location == '/login'

    def test_existing_anonymous_flow_still_works(self, app, client):
        """AC: Existing anonymous flow continues to work (backwards-compatible)."""
        from app import get_db

        response = client.post('/set-username', data={'username': 'anonuser'})
        assert response.status_code == 302

        with client.session_transaction() as sess:
            assert 'session_id' in sess
            session_id = sess['session_id']

        db = get_db()
        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('anonuser',)
        ).fetchone()
        assert user is not None
        assert user['session_id'] == session_id
        assert user['email'] is None

    def test_logged_in_user_can_make_predictions(self, app, client):
        """AC: Logged-in users can still make predictions; predictions attach
        to the email-bound session."""
        from app import get_db

        # Create a race and drivers.
        db = get_db()
        race_time = datetime.now(timezone.utc) + timedelta(hours=24)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Auth Test GP', 900, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (900,)).fetchone()['id']
        for i, code in enumerate(['A01', 'A02', 'A03']):
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (9000 + i, f'auth_driver_{i}', f'Auth Driver {i}', 900 + i, code)
            )
        db.commit()

        # Log in via magic link.
        client.post('/login/request', data={'email': 'predictor@example.com'})
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('predictor@example.com',)
        ).fetchone()
        client.get(f'/login/verify/{row["token"]}', follow_redirects=False)

        with client.session_transaction() as sess:
            session_id = sess['session_id']

        # Submit a prediction.
        response = client.post(f'/predict/{race_id}', data={
            'p1': str(9000), 'p2': str(9001), 'p3': str(9002)
        }, follow_redirects=False)
        assert response.status_code == 302

        pred = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
            (session_id, race_id)
        ).fetchone()
        assert pred is not None
        assert pred['p1_driver_id'] == 9000

    def test_debug_endpoint_hidden_in_non_dev(self, app, client, monkeypatch):
        """The debug magic-link endpoint is not exposed outside dev/test."""
        monkeypatch.setitem(app.config, 'ENVIRONMENT', 'production')
        monkeypatch.delenv('TESTING', raising=False)

        response = client.get('/debug/magic-link/anyone@example.com')
        assert response.status_code == 404
