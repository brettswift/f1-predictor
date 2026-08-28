"""Unit tests for account deletion + data export (BUD-146 / F1-14).

ACs:
- Self-serve deletion from /account (and /settings) removes the users row
  and every predictions/scores row for that user id.
- Deletion does not touch other users' rows.
- Deletion requires a confirm step (re-typing username/email).
- Data export produces JSON with profile, predictions, and scores.
- `flask export-user <id>` gives an admin the same export.
- Works for any account state (anonymous/legacy, email, OAuth-linked —
  the latter two are structurally identical rows in this schema).
"""

import json
from datetime import datetime, timezone, timedelta

import pytest


_round_counter = [0]


def _next_round():
    """Monotonic round number so parallel-in-test races/drivers never collide on id."""
    _round_counter[0] += 1
    return _round_counter[0]


def _insert_race_and_drivers(db, round_num):
    """Insert a locked race (so predictions/scores are meaningful) with 3 drivers."""
    race_time = datetime.now(timezone.utc) - timedelta(hours=1)
    db.execute(
        'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
        (f'Test GP Round {round_num}', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'locked')
    )
    race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']

    base_id = round_num * 10
    driver_ids = []
    for i, code in enumerate(['D01', 'D02', 'D03']):
        driver_id = base_id + i
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'driver_{driver_id}', f'Driver {driver_id}', driver_id, code)
        )
        driver_ids.append(driver_id)
    db.commit()
    return race_id, driver_ids


def _seed_user_with_data(db, session_id, username, email=None, legacy_user=0):
    """Insert a user plus one prediction and one score row for them."""
    db.execute(
        'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
        (session_id, username, email, legacy_user)
    )
    race_id, driver_ids = _insert_race_and_drivers(db, round_num=_next_round())
    db.execute(
        '''INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
           VALUES (?, ?, ?, ?, ?)''',
        (session_id, race_id, driver_ids[0], driver_ids[1], driver_ids[2])
    )
    db.execute(
        'INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
        (session_id, race_id, 25)
    )
    db.commit()
    return race_id


class TestDeleteUserAccountCascade:
    """Tests for the delete_user_account() helper's cascade behavior."""

    def test_hard_deletes_user_predictions_and_scores(self, app, client):
        """AC: deleting removes the users row and all predictions/scores rows for that user."""
        from app import get_db, delete_user_account

        db = get_db()
        _seed_user_with_data(db, 'sess-del-1', 'deleteme', email='deleteme@example.com')

        deleted = delete_user_account(db, 'sess-del-1')
        assert deleted is True

        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-del-1',)).fetchone() is None
        assert db.execute('SELECT COUNT(*) c FROM predictions WHERE user_id = ?', ('sess-del-1',)).fetchone()['c'] == 0
        assert db.execute('SELECT COUNT(*) c FROM scores WHERE user_id = ?', ('sess-del-1',)).fetchone()['c'] == 0

    def test_deletes_login_tokens_for_email(self, app, client):
        """Pending magic-link tokens for the deleted user's email are removed too."""
        from app import get_db, delete_user_account, create_login_token

        db = get_db()
        _seed_user_with_data(db, 'sess-del-2', 'tokenuser', email='tokenuser@example.com')
        create_login_token('tokenuser@example.com')
        assert db.execute(
            'SELECT COUNT(*) c FROM login_tokens WHERE email = ?', ('tokenuser@example.com',)
        ).fetchone()['c'] == 1

        delete_user_account(db, 'sess-del-2')

        assert db.execute(
            'SELECT COUNT(*) c FROM login_tokens WHERE email = ?', ('tokenuser@example.com',)
        ).fetchone()['c'] == 0

    def test_does_not_affect_other_users_data(self, app, client):
        """AC: other users' predictions/scores row counts are unaffected."""
        from app import get_db, delete_user_account

        db = get_db()
        _seed_user_with_data(db, 'sess-del-3', 'targetuser', email='target@example.com')
        _seed_user_with_data(db, 'sess-keep-1', 'keepuser', email='keep@example.com')

        before_preds = db.execute('SELECT COUNT(*) c FROM predictions WHERE user_id = ?', ('sess-keep-1',)).fetchone()['c']
        before_scores = db.execute('SELECT COUNT(*) c FROM scores WHERE user_id = ?', ('sess-keep-1',)).fetchone()['c']

        delete_user_account(db, 'sess-del-3')

        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-keep-1',)).fetchone() is not None
        after_preds = db.execute('SELECT COUNT(*) c FROM predictions WHERE user_id = ?', ('sess-keep-1',)).fetchone()['c']
        after_scores = db.execute('SELECT COUNT(*) c FROM scores WHERE user_id = ?', ('sess-keep-1',)).fetchone()['c']
        assert after_preds == before_preds
        assert after_scores == before_scores

    def test_works_for_legacy_anonymous_account(self, app, client):
        """AC: deletion works for legacy (email-less) accounts without special-casing."""
        from app import get_db, delete_user_account

        db = get_db()
        _seed_user_with_data(db, 'sess-del-5', 'legacyuser', email=None, legacy_user=1)

        deleted = delete_user_account(db, 'sess-del-5')
        assert deleted is True
        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-del-5',)).fetchone() is None

    def test_returns_false_for_unknown_session_id(self, app, client):
        """Deleting a nonexistent user id is a no-op, not an error."""
        from app import get_db, delete_user_account

        db = get_db()
        assert delete_user_account(db, 'no-such-session') is False


class TestAccountDeleteRoute:
    """Tests for the self-serve /account/delete route (confirm-step + auth)."""

    def _login_as(self, client, db, session_id, username, email=None):
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            (session_id, username, email)
        )
        db.commit()
        with client.session_transaction() as sess:
            sess['session_id'] = session_id

    def test_requires_login(self, app, client):
        """AC: unauthenticated users cannot delete an account; redirected to login."""
        response = client.post('/account/delete', data={'confirm': 'whatever'}, follow_redirects=False)
        assert response.status_code == 302
        assert '/login' in response.headers['Location']

    def test_wrong_confirmation_does_not_delete(self, app, client):
        """AC: confirmation step is required — a mismatched value blocks deletion."""
        from app import get_db

        db = get_db()
        self._login_as(client, db, 'sess-route-1', 'routeuser1', email='route1@example.com')

        response = client.post('/account/delete', data={'confirm': 'not-my-email'}, follow_redirects=False)
        assert response.status_code == 302
        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-route-1',)).fetchone() is not None

    def test_correct_confirmation_deletes_and_clears_session(self, app, client):
        """AC: typing the email/username exactly deletes the account."""
        from app import get_db

        db = get_db()
        self._login_as(client, db, 'sess-route-2', 'routeuser2', email='route2@example.com')

        response = client.post('/account/delete', data={'confirm': 'route2@example.com'}, follow_redirects=False)
        assert response.status_code == 302
        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-route-2',)).fetchone() is None
        assert db.execute('SELECT COUNT(*) c FROM predictions WHERE user_id = ?', ('sess-route-2',)).fetchone()['c'] == 0
        assert db.execute('SELECT COUNT(*) c FROM scores WHERE user_id = ?', ('sess-route-2',)).fetchone()['c'] == 0

        with client.session_transaction() as sess:
            assert 'session_id' not in sess

    def test_confirmation_falls_back_to_username_when_no_email(self, app, client):
        """Legacy accounts with no email confirm by typing their username instead."""
        from app import get_db

        db = get_db()
        self._login_as(client, db, 'sess-route-3', 'legacyroute', email=None)

        response = client.post('/account/delete', data={'confirm': 'legacyroute'}, follow_redirects=False)
        assert response.status_code == 302
        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-route-3',)).fetchone() is None

    def test_confirmation_is_case_insensitive(self, app, client):
        """Typing the email with different casing still confirms."""
        from app import get_db

        db = get_db()
        self._login_as(client, db, 'sess-route-4', 'routeuser4', email='Route4@Example.com')

        response = client.post('/account/delete', data={'confirm': 'route4@example.com'}, follow_redirects=False)
        assert response.status_code == 302
        assert db.execute('SELECT * FROM users WHERE session_id = ?', ('sess-route-4',)).fetchone() is None
        assert db.execute('SELECT COUNT(*) c FROM predictions WHERE user_id = ?', ('sess-route-4',)).fetchone()['c'] == 0
        assert db.execute('SELECT COUNT(*) c FROM scores WHERE user_id = ?', ('sess-route-4',)).fetchone()['c'] == 0


class TestAccountPage:
    """Tests for the /account (and /settings alias) page."""

    def test_account_page_requires_login(self, app, client):
        response = client.get('/account', follow_redirects=False)
        assert response.status_code == 302
        assert '/login' in response.headers['Location']

    def test_account_page_renders_for_logged_in_user(self, app, client):
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            ('sess-page-1', 'pageuser', 'page1@example.com')
        )
        db.commit()
        with client.session_transaction() as sess:
            sess['session_id'] = 'sess-page-1'

        response = client.get('/account')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'pageuser' in content
        assert '/account/export' in content
        assert '/account/delete' in content

    def test_settings_is_an_alias_for_account(self, app, client):
        """AC: deletion is reachable from /settings as well as /account."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            ('sess-page-2', 'settingsuser', 'settings@example.com')
        )
        db.commit()
        with client.session_transaction() as sess:
            sess['session_id'] = 'sess-page-2'

        response = client.get('/settings')
        assert response.status_code == 200
        assert 'settingsuser' in response.data.decode('utf-8')

    def test_account_page_has_privacy_link(self, app, client):
        """AC: account page links to the privacy policy."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email) VALUES (?, ?, ?)',
            ('sess-page-4', 'privacyuser', 'privacy@example.com')
        )
        db.commit()
        with client.session_transaction() as sess:
            sess['session_id'] = 'sess-page-4'

        response = client.get('/account')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '/privacy' in content
        assert 'Privacy' in content


class TestAccountExport:
    """Tests for the /account/export JSON data export."""

    def test_requires_login(self, app, client):
        response = client.get('/account/export', follow_redirects=False)
        assert response.status_code == 302
        assert '/login' in response.headers['Location']

    def test_export_contains_profile_predictions_and_scores(self, app, client):
        """AC: export is machine-readable JSON with profile, predictions, and scores."""
        from app import get_db

        db = get_db()
        _seed_user_with_data(db, 'sess-export-1', 'exportuser', email='export1@example.com')
        with client.session_transaction() as sess:
            sess['session_id'] = 'sess-export-1'

        response = client.get('/account/export')
        assert response.status_code == 200
        assert response.mimetype == 'application/json'
        assert 'attachment' in response.headers.get('Content-Disposition', '')

        data = json.loads(response.data)
        assert data['profile']['username'] == 'exportuser'
        assert data['profile']['email'] == 'export1@example.com'
        assert len(data['predictions']) == 1
        assert len(data['scores']) == 1
        assert data['scores'][0]['points'] == 25

    def test_export_only_contains_own_data(self, app, client):
        """Export must not leak another user's predictions/scores."""
        from app import get_db

        db = get_db()
        _seed_user_with_data(db, 'sess-export-2', 'exportuser2', email='export2@example.com')
        _seed_user_with_data(db, 'sess-export-3', 'otheruser', email='other@example.com')
        with client.session_transaction() as sess:
            sess['session_id'] = 'sess-export-2'

        response = client.get('/account/export')
        data = json.loads(response.data)
        assert data['profile']['username'] == 'exportuser2'
        assert len(data['predictions']) == 1
        assert len(data['scores']) == 1


class TestExportUserCli:
    """Tests for the `flask export-user <id>` admin CLI command."""

    def test_export_user_by_username(self, app, runner):
        from app import get_db

        db = get_db()
        _seed_user_with_data(db, 'sess-cli-1', 'cliuser', email='cli1@example.com')

        result = runner.invoke(args=['export-user', 'cliuser'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data['profile']['username'] == 'cliuser'
        assert len(data['predictions']) == 1
        assert len(data['scores']) == 1

    def test_export_user_by_email(self, app, runner):
        from app import get_db

        db = get_db()
        _seed_user_with_data(db, 'sess-cli-2', 'cliuser2', email='cli2@example.com')

        result = runner.invoke(args=['export-user', 'cli2@example.com'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data['profile']['username'] == 'cliuser2'

    def test_export_user_by_session_id(self, app, runner):
        from app import get_db

        db = get_db()
        _seed_user_with_data(db, 'sess-cli-3', 'cliuser3', email='cli3@example.com')

        result = runner.invoke(args=['export-user', 'sess-cli-3'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data['profile']['session_id'] == 'sess-cli-3'

    def test_export_user_unknown_identifier_errors(self, app, runner):
        result = runner.invoke(args=['export-user', 'no-such-user'])
        assert result.exit_code != 0

    def test_privacy_page_renders(self, app, client):
        """AC: privacy policy stub page is reachable and mentions export/delete."""
        response = client.get('/privacy')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Privacy' in content
        assert '/account' in content
