"""Unit tests for legacy session-cookie user migration (BUD-143 / F1-11)."""

import pytest
from datetime import datetime, timezone, timedelta


class TestLegacyUserDetection:
    """Tests for identifying legacy users."""

    def test_new_anonymous_user_is_not_legacy(self, app, client):
        """AC: Users who already have an email skip migration."""
        from app import get_db, is_legacy_user

        client.post('/set-username', data={'username': 'newanon'})
        with client.session_transaction() as sess:
            session_id = sess['session_id']

        db = get_db()
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (session_id,)).fetchone()
        assert user is not None
        assert user['legacy_user'] == 0
        assert not is_legacy_user(user)

    def test_legacy_user_has_flag_and_no_email(self, app, client):
        """Legacy users are flagged and have no email."""
        from app import get_db, is_legacy_user

        db = get_db()
        session_id = 'legacy-session-abc'
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            (session_id, 'legacyuser', None, 1)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        user = db.execute('SELECT * FROM users WHERE session_id = ?', (session_id,)).fetchone()
        assert is_legacy_user(user)
        assert user['email'] is None

    def test_email_user_is_not_legacy(self, app, client):
        """Users with an email are never legacy."""
        from app import get_db, is_legacy_user

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('email-session', 'emailuser', 'someone@example.com', 0)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'email-session'

        user = db.execute('SELECT * FROM users WHERE session_id = ?', ('email-session',)).fetchone()
        assert not is_legacy_user(user)


class TestMigrationPromptFlow:
    """Tests for the migration prompt UI."""

    def test_legacy_user_sees_banner_on_home(self, app, client):
        """AC: On first visit after migration, legacy users are prompted to add an email."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('legacy-banner', 'banneruser', None, 1)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'legacy-banner'

        response = client.get('/home')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Secure your predictions' in content
        assert 'migrate/send' in content

    def test_new_anonymous_user_does_not_see_banner(self, app, client):
        """New anonymous users should not be nagged to migrate."""
        client.post('/set-username', data={'username': 'newanon'})

        response = client.get('/home')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Secure your predictions' not in content

    def test_email_user_does_not_see_banner(self, app, client):
        """Users with email do not see the migration banner."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('email-banner', 'emailuser', 'email@example.com', 0)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'email-banner'

        response = client.get('/home')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Secure your predictions' not in content

    def test_migrate_page_renders_for_legacy_user(self, app, client):
        """The dedicated migration page prompts legacy users."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('legacy-migrate', 'migrateuser', None, 1)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'legacy-migrate'

        response = client.get('/migrate')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Keep your predictions safe' in content
        assert 'migrate/send' in content

    def test_migrate_page_redirects_email_user(self, app, client):
        """Users with email are redirected away from the migration page."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('email-migrate', 'emailuser2', 'email2@example.com', 0)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'email-migrate'

        response = client.get('/migrate', follow_redirects=False)
        assert response.status_code == 302
        assert response.location == '/home'

    def test_migrate_send_creates_login_token(self, app, client):
        """AC: Submitting an email creates a login_tokens entry and sends a magic link."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('legacy-send', 'senduser', None, 1)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'legacy-send'

        response = client.post('/migrate/send', data={'email': 'migrate@example.com'})
        assert response.status_code == 200

        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('migrate@example.com',)
        ).fetchone()
        assert token is not None
        assert token['used'] == 0

    def test_migrate_send_rejects_duplicate_email(self, app, client):
        """AC: Duplicate email handling during migration."""
        from app import get_db

        db = get_db()
        # Two users: one legacy, one already bound to the email.
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('taken-owner', 'owner', 'taken@example.com', 0)
        )
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('legacy-dup', 'dupuser', None, 1)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'legacy-dup'

        response = client.post('/migrate/send', data={'email': 'taken@example.com'}, follow_redirects=True)
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'already linked to another account' in content

        # No token should have been created for the duplicate email.
        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('taken@example.com',)
        ).fetchone()
        assert token is None


class TestPredictionPreservation:
    """Tests that migration keeps the user's history."""

    def _insert_race_and_drivers(self, db, round_num):
        race_time = datetime.now(timezone.utc) + timedelta(hours=24)
        db.execute(
            'INSERT INTO races (name, round, date, status) VALUES (?, ?, ?, ?)',
            ('Migrate GP', round_num, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'open')
        )
        race_id = db.execute('SELECT id FROM races WHERE round = ?', (round_num,)).fetchone()['id']
        driver_ids = []
        for i, code in enumerate(['M01', 'M02', 'M03']):
            driver_id = 8000 + i
            driver_ids.append(driver_id)
            db.execute(
                'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
                (driver_id, f'migrate_driver_{i}', f'Migrate Driver {i}', 800 + i, code)
            )
        db.commit()
        return race_id, driver_ids

    def test_predictions_preserved_after_migration(self, app, client):
        """AC: After verifying the magic link, the same session_id remains valid
        and the user's prediction history is preserved."""
        from app import get_db

        db = get_db()
        race_id, driver_ids = self._insert_race_and_drivers(db, round_num=800)

        # Seed a legacy user with a prediction.
        session_id = 'legacy-preds'
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            (session_id, 'preduser', None, 1)
        )
        db.execute(
            '''INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
                VALUES (?, ?, ?, ?, ?)''',
            (session_id, race_id, driver_ids[0], driver_ids[1], driver_ids[2])
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = session_id

        # Request migration link.
        client.post('/migrate/send', data={'email': 'preserve@example.com'})
        row = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('preserve@example.com',)
        ).fetchone()

        # Verify the magic link.
        response = client.get(f'/login/verify/{row["token"]}', follow_redirects=False)
        assert response.status_code == 302
        assert response.location == '/home'

        # Same session_id should still be active.
        with client.session_transaction() as sess:
            assert sess['session_id'] == session_id

        # User row now has the email and legacy flag cleared.
        user = db.execute('SELECT * FROM users WHERE session_id = ?', (session_id,)).fetchone()
        assert user['email'] == 'preserve@example.com'
        assert user['legacy_user'] == 0

        # Prediction is still tied to the same session_id.
        pred = db.execute(
            'SELECT * FROM predictions WHERE user_id = ? AND race_id = ?',
            (session_id, race_id)
        ).fetchone()
        assert pred is not None
        assert pred['p1_driver_id'] == driver_ids[0]


class TestAdminLegacyManagement:
    """Tests for the admin management of legacy users."""

    def test_admin_legacy_users_lists_only_legacy(self, app, client):
        """Admin route can list legacy users."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('admin-legacy', 'legacyadmin', None, 1)
        )
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('admin-email', 'emailadmin', 'admin@example.com', 0)
        )
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('admin-new', 'newadmin', None, 0)
        )
        db.commit()

        # Log in as admin user.
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('admin-session', 'brett', 'brett@example.com', 0)
        )
        db.commit()
        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session'

        response = client.get('/admin/legacy-users')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'legacyadmin' in content
        assert 'emailadmin' not in content
        assert 'newadmin' not in content

    def test_admin_legacy_users_rejects_non_admin(self, app, client):
        """Only admins can view the legacy users list."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('non-admin', 'notbrett', 'not@example.com', 0)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'non-admin'

        response = client.get('/admin/legacy-users', follow_redirects=True)
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Admin access only' in content

    def test_admin_legacy_users_can_trigger_migration_email(self, app, client):
        """Admin route can trigger migration emails."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('admin-session2', 'brett', 'brett@example.com', 0)
        )
        db.commit()

        with client.session_transaction() as sess:
            sess['session_id'] = 'admin-session2'

        response = client.post(
            '/admin/legacy-users',
            data={'email': 'batch@example.com'},
            follow_redirects=True
        )
        assert response.status_code == 200

        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('batch@example.com',)
        ).fetchone()
        assert token is not None


class TestLegacyCliCommands:
    """Tests for the Flask CLI management commands."""

    def test_cli_list_legacy_users(self, app, runner):
        """Management command lists legacy users."""
        from app import get_db

        db = get_db()
        db.execute(
            'INSERT INTO users (session_id, username, email, legacy_user) VALUES (?, ?, ?, ?)',
            ('cli-legacy', 'clilegacy', None, 1)
        )
        db.commit()

        result = runner.invoke(args=['list-legacy-users'])
        assert result.exit_code == 0
        assert 'clilegacy' in result.output
        assert 'cli-legacy' in result.output

    def test_cli_send_migration_email(self, app, runner):
        """Management command can trigger a migration email."""
        from app import get_db

        result = runner.invoke(args=['send-migration-email', 'cli@example.com'])
        assert result.exit_code == 0

        db = get_db()
        token = db.execute(
            'SELECT * FROM login_tokens WHERE email = ?', ('cli@example.com',)
        ).fetchone()
        assert token is not None

    def test_cli_send_migration_email_rejects_invalid_email(self, app, runner):
        """Management command rejects malformed emails."""
        result = runner.invoke(args=['send-migration-email', 'not-an-email'])
        assert result.exit_code != 0
