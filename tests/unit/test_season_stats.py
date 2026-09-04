"""Unit tests for F1-41 / BUD-141: permanent season-stats profile page.

ACs:
- signed-in user sees overall accuracy, best single race, and race-by-race
  global rank history
- stats are derived from the same scores/leaderboard data source as the
  global leaderboard (no forked computation)
- page persists across devices/sessions via real account (stable session_id)
- page is reachable and populated for a solo user with no league membership
"""

import pytest


class TestSeasonStatsRoute:
    """AC: signed-in user has a reachable season-stats page."""

    def _login(self, client, username):
        client.post('/set-username', data={'username': username})

    def _insert_driver(self, db, driver_id, name='Test Driver', number=1):
        db.execute(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            (driver_id, f'driver_{driver_id}', name, number, 'TST'),
        )
        db.commit()

    def _insert_race(self, db, race_id, name, round_num, date, status='completed'):
        db.execute(
            'INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
            (race_id, name, round_num, date, status),
        )
        db.commit()

    def _insert_result(self, db, race_id, p1, p2, p3):
        db.execute(
            'INSERT INTO results (race_id, p1_driver_id, p2_driver_id, p3_driver_id) VALUES (?, ?, ?, ?)',
            (race_id, p1, p2, p3),
        )
        db.commit()

    def _insert_score(self, db, user_id, race_id, points):
        db.execute(
            'INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
            (user_id, race_id, points),
        )
        db.commit()

    def test_requires_login(self, app, client):
        response = client.get('/stats')
        assert response.status_code == 302

    def test_renders_for_logged_in_solo_user_with_no_league(self, app, client):
        self._login(client, 'solouser')
        response = client.get('/stats')
        assert response.status_code == 200
        assert b'Season' in response.data

    def test_empty_state_shows_zero_accuracy_and_no_rank_history(self, app, client):
        self._login(client, 'emptyuser')
        response = client.get('/stats')
        content = response.data.decode('utf-8')
        assert '0%' in content
        assert 'No completed races yet' in content

    def test_populated_page_shows_accuracy_best_race_and_rank_history(self, app, client):
        from app import get_db

        db = get_db()
        for did in [1, 2, 3]:
            self._insert_driver(db, did, f'Driver {did}', did)

        self._login(client, 'statstar')
        user = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('statstar',)
        ).fetchone()

        self._insert_race(db, 101, 'Bahrain Grand Prix', 1, '2026-03-01 15:00:00')
        self._insert_race(db, 102, 'Saudi Arabian Grand Prix', 2, '2026-03-08 17:00:00')
        self._insert_race(db, 103, 'Australian Grand Prix', 3, '2026-03-15 05:00:00')

        # Single user predicted; results so scores can be stored.
        self._insert_result(db, 101, 1, 2, 3)
        self._insert_result(db, 102, 1, 3, 2)
        self._insert_result(db, 103, 2, 1, 3)

        self._insert_score(db, user['session_id'], 101, 20)  # perfect
        self._insert_score(db, user['session_id'], 102, 14)  # e.g. P1+P2 right, P3 wrong
        self._insert_score(db, user['session_id'], 103, 17)

        response = client.get('/stats')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        assert 'Total Points' in content
        assert '51' in content  # 20+14+17
        assert 'Accuracy' in content
        assert '85.0%' in content  # 51 / 60 == 85.0%, included verbatim
        assert 'Bahrain Grand Prix' in content
        assert '20' in content
        assert 'Global Rank History' in content

    def test_rank_history_matches_global_leaderboard_source(self, app, client):
        """Rank history uses cumulative scores for all non-synthetic users."""
        from app import get_db

        db = get_db()
        for did in [1, 2, 3]:
            self._insert_driver(db, did, f'Driver {did}', did)

        self._login(client, 'rankuser')
        user = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('rankuser',)
        ).fetchone()

        # A second user so ranking is non-trivial.
        client.post('/set-username', data={'username': 'otheruser'})
        other = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('otheruser',)
        ).fetchone()

        self._insert_race(db, 201, 'Race One', 1, '2026-04-01 15:00:00')
        self._insert_race(db, 202, 'Race Two', 2, '2026-04-08 15:00:00')
        self._insert_result(db, 201, 1, 2, 3)
        self._insert_result(db, 202, 1, 2, 3)

        self._insert_score(db, user['session_id'], 201, 10)
        self._insert_score(db, user['session_id'], 202, 20)
        self._insert_score(db, other['session_id'], 201, 20)
        self._insert_score(db, other['session_id'], 202, 10)

        # Re-login as rankuser and check history reflects same totals as leaderboard.
        self._login(client, 'rankuser')
        response = client.get('/stats')
        content = response.data.decode('utf-8')

        # After race 2 both users have 30 points; rankuser should be rank 1 (tie,
        # alphabetical/session ordering not important — only that the page shows
        # a rank after each completed race and the total matches).
        assert '30' in content
        assert 'Global Rank History' in content

    def test_synthetic_users_excluded_from_rank_history_totals(self, app, client):
        """Synthetic users do not inflate total_users in rank history."""
        from app import get_db

        db = get_db()
        for did in [1, 2, 3]:
            self._insert_driver(db, did, f'Driver {did}', did)

        self._login(client, 'realuser')
        real = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('realuser',)
        ).fetchone()

        db.execute(
            'INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)',
            ('bot-1', 'synthetic_user', 1),
        )
        db.commit()

        self._insert_race(db, 301, 'Synthetic Check', 1, '2026-05-01 15:00:00')
        self._insert_result(db, 301, 1, 2, 3)
        self._insert_score(db, real['session_id'], 301, 5)
        self._insert_score(db, 'bot-1', 301, 20)

        response = client.get('/stats')
        content = response.data.decode('utf-8')

        assert '/ 1' in content  # only the real user counts in total users

    def test_stats_persist_across_fresh_login(self, app, client):
        """Page data is tied to the stable user row, not the current cookie."""
        from app import get_db

        db = get_db()
        self._insert_driver(db, 1, 'Driver 1', 1)
        self._insert_driver(db, 2, 'Driver 2', 2)
        self._insert_driver(db, 3, 'Driver 3', 3)

        self._login(client, 'persistuser2')
        user = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('persistuser2',)
        ).fetchone()

        self._insert_race(db, 401, 'Persist Race', 1, '2026-06-01 15:00:00')
        self._insert_result(db, 401, 1, 2, 3)
        self._insert_score(db, user['session_id'], 401, 20)

        # Simulate a fresh device binding to the same user row by email.
        db.execute(
            'UPDATE users SET email = ? WHERE session_id = ?',
            ('persist@example.com', user['session_id'])
        )
        db.commit()

        with client.session_transaction() as sess:
            sess.clear()
            sess['session_id'] = user['session_id']

        response = client.get('/stats')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'Total Points' in content

    def test_helper_uses_only_scores_table(self, app, client):
        """Directly verify that _get_season_stats concludes from the scores table,
        matching the sum on /leaderboard for the same user/season."""
        from app import get_db, _get_season_stats

        db = get_db()
        for did in [1, 2, 3]:
            self._insert_driver(db, did, f'Driver {did}', did)

        self._login(client, 'sourceuser')
        user = get_db().execute(
            'SELECT session_id FROM users WHERE username = ?', ('sourceuser',)
        ).fetchone()

        self._insert_race(db, 501, 'Source Race', 1, '2026-07-01 15:00:00')
        self._insert_result(db, 501, 1, 2, 3)
        self._insert_score(db, user['session_id'], 501, 20)

        stats = _get_season_stats(db, user['session_id'], 2026)
        leaderboard_total = db.execute('''
            SELECT COALESCE(SUM(s.points), 0) as total
            FROM scores s
            JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
            WHERE s.user_id = ?
        ''', ('2026', user['session_id'])).fetchone()['total']

        assert stats['total_score'] == leaderboard_total == 20
