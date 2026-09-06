"""BUD-153 / F1-23: Dual ranking modes (Total + Average) on leaderboard."""

import pytest


class TestDualRankingModes:
    """F1-23: Total and Average ranking modes on the leaderboard."""

    def _setup_users_with_races(self, db):
        """Create three users with varying race counts in 2026.
        user-a: 3 races (qualifies for Average)
        user-b: 2 races (boundary — qualifies at >=2)
        user-c: 1 race  (below threshold, only in Total)
        """
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('user-a', 'alice'))
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('user-b', 'bob'))
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('user-c', 'carol'))

        for rid, rname, rnd in [(9001, 'Race 1', 1), (9002, 'Race 2', 2), (9003, 'Race 3', 3)]:
            db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                       (rid, rname, rnd, '2026-03-%02d 14:00:00' % rnd, 'completed'))

        # alice: 3 races, 30+25+20 = 75 total, avg 25.0
        for rid in [9001, 9002, 9003]:
            db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                       ('user-a', rid, 35 - (rid - 9000) * 5))
        # bob: 2 races, 15+10 = 25 total, avg 12.5
        for rid in [9001, 9002]:
            db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                       ('user-b', rid, 20 - (rid - 9000) * 5))
        # carol: 1 race, 50 total, avg 50.0 (but <2 races, not ranked in Average)
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('user-c', 9003, 50))
        db.commit()

    def test_bud153_total_mode_is_default(self, app, client):
        """BUD-153: leaderboard defaults to Total mode when no preference is set."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        # All three users should appear in Total mode.
        assert 'alice' in content
        assert 'bob' in content
        assert 'carol' in content
        # Total column header (not Avg).
        assert 'Total' in content

    def test_bud153_average_mode_sorts_by_avg_points(self, app, client):
        """BUD-153: Average mode sorts by avg_points descending."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard?mode=average')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        # alice avg 25.0, bob avg 12.5 — alice should appear before bob.
        ap = content.find('alice')
        bp = content.find('bob')
        assert ap < bp, 'alice (avg 25.0) should rank ahead of bob (avg 12.5)'

    def test_bud153_carol_below_threshold_not_ranked_in_average(self, app, client):
        """BUD-153: user with 1 scored race not in ranked Average list."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard?mode=average')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        # carol has 1 race — should appear below threshold.
        assert 'Below threshold' in content
        assert 'carol' in content
        # carol's race count should be shown.
        assert '1 race' in content

    def test_bud153_carol_appears_in_total_mode(self, app, client):
        """BUD-153: user with 1 scored race still appears in Total mode."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard?mode=total')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        assert 'carol' in content
        # No "Below threshold" section in Total mode.
        assert 'Below threshold' not in content

    def test_bud153_boundary_two_races_qualified_in_average(self, app, client):
        """BUD-153: user with exactly 2 scored races appears in Average (>=2, not >2)."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard?mode=average')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        # bob has 2 races — should be in the ranked section.
        # The ranked section comes before "Below threshold".
        threshold_pos = content.find('Below threshold')
        bob_pos = content.find('bob')
        assert bob_pos != -1
        # bob appears before the unranked section (he's ranked).
        assert bob_pos < threshold_pos, 'bob (2 races) should be ranked, not below threshold'

    def test_bud153_mode_persists_without_query_param(self, app, client):
        """BUD-153: selected mode persists across reloads without ?mode= param.

        Sets mode=average via query-string, then reloads /leaderboard
        (no ?mode= param) and asserts Average mode is still active.
        """
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        # Step 1: set mode to average via query-string.
        response = client.get('/leaderboard?mode=average')
        assert response.status_code == 200
        # Avg header confirms mode.
        assert 'Avg' in response.data.decode('utf-8')

        # Step 2: reload without ?mode= param — persisted pref should apply.
        response2 = client.get('/leaderboard')
        assert response2.status_code == 200
        content2 = response2.data.decode('utf-8')
        assert 'Avg' in content2, 'Average mode should persist without ?mode='
        # "In Average mode" indicator.
        assert 'Below threshold' in content2, 'Expected Average-mode unranked section'

        # Step 3: switch back to total via query-string.
        response3 = client.get('/leaderboard?mode=total')
        assert response3.status_code == 200
        # Step 4: reload without mode — should be back to total.
        response4 = client.get('/leaderboard')
        assert response4.status_code == 200
        content4 = response4.data.decode('utf-8')
        assert 'Total' in content4

    def test_bud153_mode_toggle_links_present(self, app, client):
        """BUD-153: mode-toggle Total/Avg links are present on the page."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'

        response = client.get('/leaderboard?mode=total')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'mode=t' in content or 'mode=' in content  # at least one mode link

    def test_bud153_mode_persists_across_requests_for_different_user(self, app, client):
        """BUD-153: each user's ranking-mode preference is independent."""
        from app import get_db
        db = get_db()
        self._setup_users_with_races(db)

        # user-a sets average.
        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'
        client.get('/leaderboard?mode=average')

        # user-b has no preference — should default to total.
        with client.session_transaction() as sess:
            sess['session_id'] = 'user-b'
        response = client.get('/leaderboard')
        content = response.data.decode('utf-8')
        # user-b should see Total mode (default).
        assert 'Total' in content

        # user-a reloads — should still see Average.
        with client.session_transaction() as sess:
            sess['session_id'] = 'user-a'
        response2 = client.get('/leaderboard')
        assert 'Avg' in response2.data.decode('utf-8')


class TestLeaderboardWithLeague:
    """F1-23: Average mode within league-scoped views.

    League standings (BUD-152 / F1-22) are a pure membership filter over
    the same season-year window as the global view. The average uses the
    exact same join as total — same window, same races.
    """

    def _setup_league_data(self, db):
        """Create a league with two members and races in 2026.

        Races: R1 (round 1), R2 (round 2), R3 (round 3) — all 2026.
        xavier: 3 races (30+20+10=60, avg=20.0) — qualifies for avg.
        yara:  1 race  (40 pts, avg=40.0) — below threshold in avg mode.
        """
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('luser-x', 'xavier'))
        db.execute('INSERT INTO users (session_id, username) VALUES (?, ?)',
                   ('luser-y', 'yara'))

        for rid, rname, rnd in [(9101, 'League R1', 1), (9102, 'League R2', 2), (9103, 'League R3', 3)]:
            db.execute('INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)',
                       (rid, rname, rnd, '2026-0%d-15 14:00:00' % rnd, 'completed'))

        # xavier: 3 races, 30+20+10 = 60 total, 20.0 avg.
        for rid, pts in [(9101, 30), (9102, 20), (9103, 10)]:
            db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                       ('luser-x', rid, pts))
        # yara: 1 race, 40 pts.
        db.execute('INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)',
                   ('luser-y', 9103, 40))

        db.execute('''INSERT INTO leagues (id, name, emoji_or_color, start_round, whole_season, admin_user_id)
                      VALUES (?, ?, ?, ?, ?, ?)''',
                   (910, 'Test League', '🏎️', 2, 0, 'luser-x'))
        db.execute('INSERT INTO league_members (league_id, user_id, is_admin) VALUES (?, ?, ?)',
                   (910, 'luser-x', 1))
        db.execute('INSERT INTO league_members (league_id, user_id, is_admin) VALUES (?, ?, ?)',
                   (910, 'luser-y', 0))
        db.commit()

    def test_bud153_league_global_average_same_window(self, app, client):
        """BUD-153: league standings use same season-year window as global.

        Both global and league Average are computed over the same set of
        races (year-filtered). The league's start_round is a membership
        concept, not a scoring window — F1-22 is a pure member filter.
        """
        from app import get_db
        db = get_db()
        self._setup_league_data(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'luser-x'

        # Global Average — all 2026 races for xavier: 30+20+10 = 60, avg=20.0.
        response = client.get('/leaderboard?mode=average')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert '20.0' in content

        # League Average — same year window, same avg (pure member filter).
        response2 = client.get('/leaderboard?league=910&mode=average')
        assert response2.status_code == 200
        content2 = response2.data.decode('utf-8')
        assert '20.0' in content2
        assert 'xavier' in content2

    def test_bud153_league_yara_below_threshold_in_avg_mode(self, app, client):
        """BUD-153: yara has 1 race, not ranked in league Average."""
        from app import get_db
        db = get_db()
        self._setup_league_data(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'luser-x'

        response = client.get('/leaderboard?league=910&mode=average')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        assert 'Below threshold' in content
        assert 'yara' in content

    def test_bud153_league_total_mode_shows_all_members(self, app, client):
        """BUD-153: league Total mode shows all members regardless of race count."""
        from app import get_db
        db = get_db()
        self._setup_league_data(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'luser-x'

        response = client.get('/leaderboard?league=910&mode=total')
        assert response.status_code == 200
        content = response.data.decode('utf-8')

        assert 'xavier' in content
        assert 'yara' in content
        assert 'Below threshold' not in content

    def test_bud153_league_mode_toggle_preserves_league_param(self, app, client):
        """BUD-153: mode toggle links preserve the league query parameter."""
        from app import get_db
        db = get_db()
        self._setup_league_data(db)

        with client.session_transaction() as sess:
            sess['session_id'] = 'luser-x'

        response = client.get('/leaderboard?league=910&mode=total')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        # Toggle links should include league=910.
        assert 'league=910' in content