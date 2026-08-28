"""Unit tests for minimal profile: display name, avatar, favorite driver (F1-13 / BUD-145).

ACs:
- users table has editable display_name, avatar_emoji, favorite_driver_id fields
- logged-in user can edit all three via /profile and see changes persist across a
  fresh login (not just the current session)
- display_name defaults to username, never blank
- leaderboard / prediction lists show display_name + avatar instead of raw username
- changing display_name does not break FK relationships or historical rows
"""

import pytest


class TestProfileSchema:
    """AC: users table has the new profile columns."""

    def test_users_table_has_profile_columns(self, app, client):
        from app import get_db

        db = get_db()
        columns = {row[1] for row in db.execute('PRAGMA table_info(users)').fetchall()}
        assert 'display_name' in columns
        assert 'avatar_emoji' in columns
        assert 'favorite_driver_id' in columns


class TestDisplayNameFallback:
    """AC: display_name defaults to username and is never blank."""

    def test_display_name_for_falls_back_to_username(self, app, client):
        from app import get_db, display_name_for

        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('u-fallback', 'plainuser'))
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-fallback',)).fetchone()

        assert display_name_for(user) == 'plainuser'

    def test_display_name_for_uses_display_name_when_set(self, app, client):
        from app import get_db, display_name_for

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, display_name) VALUES (?, ?, ?)',
            ('u-named', 'plainuser', 'Cool Racer'),
        )
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-named',)).fetchone()

        assert display_name_for(user) == 'Cool Racer'

    def test_display_name_for_never_blank_after_whitespace_only(self, app, client):
        """An empty-string display_name (not NULL) still falls back to username."""
        from app import get_db, display_name_for

        db = get_db()
        db.execute(
            "INSERT INTO users (session_id, username, display_name) VALUES (?, ?, '')",
            ('u-blank', 'plainuser'),
        )
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-blank',)).fetchone()

        assert display_name_for(user) == 'plainuser'


class TestAvatarFallback:
    """AC: avatar falls back to a deterministic default (simple identicon) when unset."""

    def test_avatar_for_uses_chosen_emoji(self, app, client):
        from app import get_db, avatar_for

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, avatar_emoji) VALUES (?, ?, ?)',
            ('u-avatar', 'avataruser', '🔥'),
        )
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-avatar',)).fetchone()

        assert avatar_for(user) == '🔥'

    def test_avatar_for_defaults_when_unset(self, app, client):
        from app import get_db, avatar_for, DEFAULT_AVATAR_EMOJIS

        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('u-noavatar', 'noavataruser'))
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-noavatar',)).fetchone()

        assert avatar_for(user) in DEFAULT_AVATAR_EMOJIS

    def test_avatar_for_default_is_deterministic(self, app, client):
        """Same key always yields the same default avatar."""
        from app import get_db, avatar_for

        db = get_db()
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('u-stable-key', 'stableuser'))
        db.commit()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('u-stable-key',)).fetchone()

        assert avatar_for(user) == avatar_for(user)


class TestProfileRoute:
    """AC: logged-in user can edit display name, avatar, favorite driver via /profile."""

    def _login(self, client, username):
        client.post('/set-username', data={'username': username})

    def _insert_driver(self, db, driver_id=1, name='Test Driver'):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'driver_{driver_id}', name, driver_id, 'TST'),
        )
        db.commit()

    def test_requires_login(self, app, client):
        response = client.get('/profile')
        assert response.status_code == 302

    def test_get_renders_form_for_logged_in_user(self, app, client):
        self._login(client, 'formuser')
        response = client.get('/profile')
        assert response.status_code == 200
        assert b'Display name' in response.data

    def test_post_updates_display_name_and_persists(self, app, client):
        from app import get_db

        self._login(client, 'persistuser')
        db = get_db()

        response = client.post('/profile', data={
            'display_name': 'New Name',
            'avatar_emoji': '',
            'favorite_driver_id': '',
        })
        assert response.status_code == 302

        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('persistuser',)
        ).fetchone()
        assert user['display_name'] == 'New Name'

    def test_post_sets_avatar_and_favorite_driver(self, app, client):
        from app import get_db

        db = get_db()
        self._insert_driver(db, driver_id=5, name='Fast Driver')
        self._login(client, 'avataruser2')

        response = client.post('/profile', data={
            'display_name': '',
            'avatar_emoji': '🔥',
            'favorite_driver_id': '5',
        })
        assert response.status_code == 302

        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('avataruser2',)
        ).fetchone()
        assert user['avatar_emoji'] == '🔥'
        assert user['favorite_driver_id'] == 5

    def test_post_rejects_invalid_favorite_driver(self, app, client):
        from app import get_db

        self._login(client, 'baddriveruser')
        db = get_db()

        response = client.post('/profile', data={
            'display_name': '',
            'avatar_emoji': '',
            'favorite_driver_id': '99999',
        })
        assert response.status_code == 302

        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('baddriveruser',)
        ).fetchone()
        assert user['favorite_driver_id'] is None

    def test_post_rejects_avatar_not_in_choices(self, app, client):
        from app import get_db

        self._login(client, 'badavataruser')
        db = get_db()

        response = client.post('/profile', data={
            'display_name': '',
            'avatar_emoji': '💀 not-a-real-choice',
            'favorite_driver_id': '',
        })
        assert response.status_code == 302

        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('badavataruser',)
        ).fetchone()
        assert user['avatar_emoji'] is None

    def test_blank_display_name_clears_override_and_falls_back_to_username(self, app, client):
        """Submitting a blank display name clears any previous override."""
        from app import get_db

        self._login(client, 'clearname')
        db = get_db()
        db.execute("UPDATE users SET display_name = 'Old Name' WHERE username = 'clearname'")
        db.commit()

        client.post('/profile', data={
            'display_name': '',
            'avatar_emoji': '',
            'favorite_driver_id': '',
        })

        user = db.execute(
            'SELECT * FROM users WHERE username = ?', ('clearname',)
        ).fetchone()
        assert user['display_name'] is None


class TestProfilePersistsAcrossFreshLogin:
    """AC: changes persist across a fresh login, not just the current session."""

    def test_profile_fields_survive_new_session_lookup(self, app, client):
        from app import get_db

        client.post('/set-username', data={'username': 'freshlogin'})
        client.post('/profile', data={
            'display_name': 'Persisted Name',
            'avatar_emoji': '🌟',
            'favorite_driver_id': '',
        })

        db = get_db()
        user_row = db.execute(
            'SELECT session_id FROM users WHERE username = ?', ('freshlogin',)
        ).fetchone()

        # Simulate a brand-new client session binding to the same user row
        # (mirrors what happens on a fresh login: session_id restored, not
        # re-derived from in-memory state).
        with client.session_transaction() as sess:
            sess.clear()
            sess['session_id'] = user_row['session_id']

        response = client.get('/home')
        assert response.status_code == 200
        assert b'Persisted Name' in response.data
        assert '🌟'.encode('utf-8') in response.data


class TestLeaderboardAndPredictionsShowDisplayName:
    """AC: leaderboard and per-race prediction lists show display_name/avatar."""

    def _login(self, client, username):
        client.post('/set-username', data={'username': username})

    def test_leaderboard_shows_display_name_instead_of_username(self, app, client):
        from app import get_db

        self._login(client, 'lbuser')
        db = get_db()
        db.execute("UPDATE users SET display_name = 'Leaderboard Star' WHERE username = 'lbuser'")
        db.commit()

        response = client.get('/leaderboard')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Leaderboard Star' in content

    def test_race_detail_predictions_show_display_name(self, app, client):
        from app import get_db
        from datetime import datetime, timezone, timedelta

        db = get_db()
        for i, code in enumerate(['D1', 'D2', 'D3']):
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (900 + i, f'race_driver_{900+i}', f'Race Driver {900+i}', 900 + i, code),
            )
        past_time = datetime.now(timezone.utc) - timedelta(hours=2)
        db.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (9001, 'Display Name GP', 901, past_time.strftime('%Y-%m-%d %H:%M:%S'), 'locked'),
        )
        db.commit()

        self._login(client, 'raceuser')
        db.execute("UPDATE users SET display_name = 'Race Fan' WHERE username = 'raceuser'")
        user = db.execute('SELECT session_id FROM users WHERE username = ?', ('raceuser',)).fetchone()
        db.execute(
            'INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (user['session_id'], 9001, 900, 901, 902),
        )
        db.commit()

        response = client.get('/race/9001', follow_redirects=True)
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Race Fan' in content
