"""Tests for BUD-175: /sim admin page."""
import pytest


class TestSimAdmin:
    """Tests for the unlisted /sim admin control page."""

    def _login(self, client, username):
        client.post('/set-username', data={'username': username})

    def test_sim_redirects_anon_to_login(self, app, client):
        """Unauthenticated users get redirected (302 to index)."""
        response = client.get('/sim', follow_redirects=False)
        assert response.status_code == 302

    def test_sim_blocks_non_admin(self, app, client):
        """Non-admin users are blocked."""
        self._login(client, 'plainuser')
        response = client.get('/sim', follow_redirects=False)
        assert response.status_code == 302

    def test_sim_renders_for_admin(self, app, client):
        """Admin sees the /sim page with buttons for each control."""
        self._login(client, 'brett')
        response = client.get('/sim', follow_redirects=True)
        assert response.status_code == 200
        body = response.data.decode()
        assert 'Sim Admin' in body
        assert 'Fast-Forward' in body
        assert 'Reset Season' in body

    def test_fast_forward_post(self, app, client):
        """POST /admin/fast-forward succeeds for admin."""
        from app import get_db
        get_db()  # ensure DB initialized

        self._login(client, 'brett')
        response = client.post('/admin/fast-forward', follow_redirects=True)
        assert response.status_code == 200
        assert b'Fast-forward' in response.data

    def test_reset_season_post(self, app, client):
        """POST /admin/reset-season succeeds for admin."""
        from app import get_db
        get_db()

        self._login(client, 'brett')
        response = client.post('/admin/reset-season', follow_redirects=True)
        assert response.status_code == 200
        assert b'Season reset' in response.data

    def test_sim_controls_not_in_public_nav(self, app, client):
        """No /sim link appears in the nav for anyone."""
        self._login(client, 'brett')
        response = client.get('/home', follow_redirects=True)
        body = response.data.decode()
        # The base template includes nav links — /sim should not be among them
        assert '/sim' not in body or 'href="/sim"' not in body

    def test_fast_forward_blocks_non_admin(self, app, client):
        """Non-admin cannot POST /admin/fast-forward."""
        self._login(client, 'notadmin')
        response = client.post('/admin/fast-forward', follow_redirects=False)
        assert response.status_code == 302

    def test_reset_season_blocks_non_admin(self, app, client):
        """Non-admin cannot POST /admin/reset-season."""
        self._login(client, 'notadmin')
        response = client.post('/admin/reset-season', follow_redirects=False)
        assert response.status_code == 302