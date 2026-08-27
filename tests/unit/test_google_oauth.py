"""Unit tests for Google OAuth sign-in (BUD-144 / F1-12)."""

import pytest
import responses


class TestGoogleOAuth:
    """Tests for the optional Google OAuth sign-in flow."""

    @pytest.fixture(autouse=True)
    def google_oauth_config(self, app, monkeypatch):
        """Set deterministic Google OAuth config for every test."""
        monkeypatch.setitem(app.config, 'GOOGLE_OAUTH_CLIENT_ID', 'test-client-id')
        monkeypatch.setitem(app.config, 'GOOGLE_OAUTH_CLIENT_SECRET', 'test-client-secret')
        monkeypatch.setitem(
            app.config,
            'GOOGLE_OAUTH_REDIRECT_URI',
            'http://localhost/login/oauth/google/callback',
        )

    def _mock_google_token_exchange(self, rsps, access_token='test-access-token'):
        """Register mocks for Google's token and userinfo endpoints."""
        from app import GOOGLE_TOKEN_ENDPOINT, GOOGLE_USERINFO_ENDPOINT

        rsps.add(
            responses.POST,
            GOOGLE_TOKEN_ENDPOINT,
            json={
                'access_token': access_token,
                'token_type': 'Bearer',
                'expires_in': 3600,
            },
            status=200,
        )
        rsps.add(
            responses.GET,
            GOOGLE_USERINFO_ENDPOINT,
            json={
                'sub': 'google-user-123',
                'email': 'oauth@example.com',
                'email_verified': True,
                'name': 'OAuth User',
            },
            status=200,
        )

    @responses.activate
    def test_oauth_initiation_redirects_to_google(self, client):
        """AC: Users can initiate Google OAuth from /login/oauth/google."""
        response = client.get('/login/oauth/google', follow_redirects=False)
        assert response.status_code == 302

        location = response.headers['Location']
        assert location.startswith('https://accounts.google.com/o/oauth2/v2/auth')
        assert 'client_id=test-client-id' in location
        assert 'response_type=code' in location
        assert 'scope=openid+email+profile' in location
        assert 'redirect_uri=' in location

        # A CSRF state token should be stored in the session.
        with client.session_transaction() as sess:
            assert 'oauth_state' in sess
            assert len(sess['oauth_state']) > 0

    def test_oauth_initiation_without_config_redirects_to_login(self, app, client, monkeypatch):
        """If Google OAuth is not configured, initiation redirects to the login page."""
        monkeypatch.setitem(app.config, 'GOOGLE_OAUTH_CLIENT_ID', '')

        response = client.get('/login/oauth/google', follow_redirects=False)
        assert response.status_code == 302
        assert response.headers['Location'] == '/login'

    @responses.activate
    def test_oauth_callback_creates_new_user(self, app, client):
        """AC: OAuth callback creates a new user when no matching email exists."""
        from app import get_db

        self._mock_google_token_exchange(responses)

        with client.session_transaction() as sess:
            sess['oauth_state'] = 'test-state'

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/home'

        with client.session_transaction() as sess:
            assert 'session_id' in sess
            session_id = sess['session_id']

        db = get_db()
        user = db.execute(
            'SELECT * FROM users WHERE email = ?', ('oauth@example.com',)
        ).fetchone()
        assert user is not None
        assert user['session_id'] == session_id
        assert user['username'] == 'oauth'

    @responses.activate
    def test_oauth_callback_links_existing_user_by_email(self, app, client):
        """AC: OAuth callback links to an existing user with a matching email."""
        from app import get_db

        db = get_db()
        existing_session_id = 'existing-session-id'
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            (existing_session_id, 'existinguser', 'oauth@example.com'),
        )
        db.commit()

        self._mock_google_token_exchange(responses)

        with client.session_transaction() as sess:
            sess['oauth_state'] = 'test-state'

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/home'

        with client.session_transaction() as sess:
            assert sess['session_id'] == existing_session_id

        user_count = db.execute(
            'SELECT COUNT(*) FROM users WHERE email = ?', ('oauth@example.com',)
        ).fetchone()[0]
        assert user_count == 1

    @responses.activate
    def test_oauth_callback_persists_session(self, app, client):
        """AC: After OAuth success, the user has a persistent session cookie."""
        self._mock_google_token_exchange(responses)

        with client.session_transaction() as sess:
            sess['oauth_state'] = 'test-state'

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302

        set_cookie = response.headers.get('Set-Cookie', '')
        assert 'session=' in set_cookie
        assert 'Max-Age=' in set_cookie or 'Expires=' in set_cookie

        # Subsequent requests are authenticated.
        home_response = client.get('/home', follow_redirects=False)
        assert home_response.status_code == 200

    @responses.activate
    def test_oauth_callback_handles_duplicate_anonymous_account(self, app, client):
        """AC: Duplicate-account handling switches to the existing email-bound user."""
        from app import get_db

        db = get_db()
        existing_session_id = 'existing-session-id'
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            (existing_session_id, 'existinguser', 'oauth@example.com'),
        )
        db.commit()

        # Start an anonymous session.
        anon_response = client.post('/set-username', data={'username': 'anonuser'}, follow_redirects=False)
        assert anon_response.status_code == 302

        with client.session_transaction() as sess:
            anon_session_id = sess['session_id']
            assert anon_session_id != existing_session_id
            sess['oauth_state'] = 'test-state'

        self._mock_google_token_exchange(responses)

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/home'

        with client.session_transaction() as sess:
            assert sess['session_id'] == existing_session_id

        # The anonymous user row still exists; the existing user row is unchanged.
        anon_user = db.execute(
            'SELECT * FROM users WHERE session_id = ?', (anon_session_id,)
        ).fetchone()
        assert anon_user is not None
        assert anon_user['email'] is None

    @responses.activate
    def test_magic_link_and_google_same_email(self, app, client):
        """AC: A user can have both magic-link and Google login methods bound to the same email."""
        from app import get_db

        db = get_db()

        # Create a user via magic link.
        client.post('/login/request', data={'email': 'both@example.com'})
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('both@example.com',)
        ).fetchone()
        client.get(f'/login/verify/{row["token"]}', follow_redirects=False)

        with client.session_transaction() as sess:
            magic_session_id = sess['session_id']
            sess['oauth_state'] = 'test-state'

        # Now sign in with Google using the same email.
        responses.add(
            responses.POST,
            'https://oauth2.googleapis.com/token',
            json={'access_token': 'tok', 'token_type': 'Bearer'},
            status=200,
        )
        responses.add(
            responses.GET,
            'https://openidconnect.googleapis.com/v1/userinfo',
            json={'sub': 'g2', 'email': 'both@example.com', 'email_verified': True},
            status=200,
        )

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302

        with client.session_transaction() as sess:
            assert sess['session_id'] == magic_session_id

    @responses.activate
    def test_oauth_callback_rejects_invalid_state(self, client):
        """AC: Callback validates the OAuth state parameter (CSRF protection)."""
        with client.session_transaction() as sess:
            sess['oauth_state'] = 'real-state'

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=wrong-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/login'

    @responses.activate
    def test_oauth_callback_rejects_missing_code(self, client):
        """AC: Callback without an authorization code redirects to login."""
        with client.session_transaction() as sess:
            sess['oauth_state'] = 'test-state'

        response = client.get(
            '/login/oauth/google/callback?state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/login'

    @responses.activate
    def test_oauth_callback_rejects_token_exchange_failure(self, client):
        """AC: Failed token exchange redirects to login without creating a user."""
        from app import get_db, GOOGLE_TOKEN_ENDPOINT

        responses.add(responses.POST, GOOGLE_TOKEN_ENDPOINT, json={'error': 'invalid_grant'}, status=400)

        with client.session_transaction() as sess:
            sess['oauth_state'] = 'test-state'

        response = client.get(
            '/login/oauth/google/callback?code=auth-code&state=test-state',
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers['Location'] == '/login'

        db = get_db()
        user = db.execute('SELECT * FROM users WHERE email = ?', ('oauth@example.com',)).fetchone()
        assert user is None

    @responses.activate
    def test_anonymous_flow_still_works(self, app, client):
        """AC: The existing anonymous username flow continues to work."""
        from app import get_db

        response = client.post('/set-username', data={'username': 'anonuser'}, follow_redirects=False)
        assert response.status_code == 302

        with client.session_transaction() as sess:
            assert 'session_id' in sess
            session_id = sess['session_id']

        db = get_db()
        user = db.execute('SELECT * FROM users WHERE username = ?', ('anonuser',)).fetchone()
        assert user is not None
        assert user['session_id'] == session_id
        assert user['email'] is None

    def test_no_password_column_in_users(self, app):
        """AC: No passwords are stored for OAuth (or any) users."""
        from app import get_db

        db = get_db()
        columns = {row[1] for row in db.execute('PRAGMA table_info(users)').fetchall()}
        assert 'password' not in columns
        assert 'password_hash' not in columns
