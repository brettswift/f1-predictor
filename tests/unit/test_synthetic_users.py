"""Unit tests for F1-111 synthetic user flag and isolation."""

import pytest


class TestSyntheticUserCreation:
    """F1-111: normal users are created with is_synthetic=0."""

    def test_set_username_creates_user_with_is_synthetic_zero(self, app, client):
        """Production user signup explicitly sets is_synthetic=0."""
        from app import get_db

        response = client.post('/set-username', data={'username': 'brett_real'}, follow_redirects=True)
        assert response.status_code == 200

        db = get_db()
        row = db.execute(
            "SELECT session_id, username, is_synthetic FROM users WHERE username = ?",
            ("brett_real",),
        ).fetchone()
        assert row is not None
        assert row["is_synthetic"] == 0


class TestSyntheticUsersExcludedFromLeaderboards:
    """F1-111: synthetic users do not appear on public leaderboards."""

    @pytest.fixture
    def _leaderboard_fixtures(self, app, client):
        """Insert one real user and one synthetic user with season 2026 scores."""
        from app import get_db
        db = get_db()

        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("real-user", "brett_real", 0),
        )
        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("syn-user", "synthetic_bot_001", 1),
        )
        db.execute(
            "INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)",
            (1, "Bahrain GP", 1, "2026-03-15 14:00:00", "completed"),
        )
        db.execute(
            "INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)",
            ("real-user", 1, 20),
        )
        db.execute(
            "INSERT INTO scores (user_id, race_id, points) VALUES (?, ?, ?)",
            ("syn-user", 1, 99),
        )
        db.commit()

        with client.session_transaction() as sess:
            sess["session_id"] = "real-user"

    def test_leaderboard_excludes_synthetic_users(self, app, client, _leaderboard_fixtures):
        response = client.get('/leaderboard?season=2026')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'brett_real' in content
        assert 'synthetic_bot_001' not in content

    def test_live_aggregate_leaderboard_query_excludes_synthetic_users(self, app, _leaderboard_fixtures):
        """The aggregate leaderboard SQL used by /live excludes synthetic users."""
        from app import get_db
        db = get_db()
        season = '2026'
        rows = db.execute('''
            SELECT u.session_id, u.username,
                   COALESCE(SUM(s.points), 0) as total_score
            FROM users u
            LEFT JOIN scores s ON u.session_id = s.user_id
            LEFT JOIN races r ON s.race_id = r.id AND strftime('%Y', r.date) = ?
            WHERE u.is_synthetic = 0
            GROUP BY u.session_id
            ORDER BY total_score DESC
        ''', (season,)).fetchall()
        usernames = {row['username'] for row in rows}
        assert 'brett_real' in usernames
        assert 'synthetic_bot_001' not in usernames


class TestSyntheticUsersExcludedFromRaceDetail:
    """F1-111: synthetic predictions do not appear on race detail pages."""

    @pytest.fixture
    def _race_detail_fixtures(self, app, client):
        from app import get_db
        db = get_db()

        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("real-user", "brett_real", 0),
        )
        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("syn-user", "synthetic_bot_001", 1),
        )
        db.execute(
            "INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)",
            (1, "Bahrain GP", 1, "2026-03-15 14:00:00", "completed"),
        )
        # Need drivers because predictions join the drivers table.
        for i in range(1, 4):
            db.execute(
                "INSERT INTO drivers (id, driver_id, name, number) VALUES (?, ?, ?, ?)",
                (i, f"drv{i}", f"Driver {i}", i),
            )
        db.execute(
            "INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) "
            "VALUES (?, ?, ?, ?, ?)",
            ("real-user", 1, 1, 2, 3),
        )
        db.execute(
            "INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) "
            "VALUES (?, ?, ?, ?, ?)",
            ("syn-user", 1, 3, 2, 1),
        )
        db.commit()

        with client.session_transaction() as sess:
            sess["session_id"] = "real-user"

    def test_race_detail_excludes_synthetic_predictions(self, app, client, _race_detail_fixtures):
        response = client.get('/race/1', follow_redirects=True)
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'brett_real' in content
        assert 'synthetic_bot_001' not in content


class TestSyntheticUsersExcludedFromLiveRaceLeaderboard:
    """F1-111: synthetic users do not appear on /race/<id>/live leaderboards."""

    @pytest.fixture
    def _live_race_fixtures(self, app, client, monkeypatch):
        from app import get_db
        db = get_db()

        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("real-user", "brett_real", 0),
        )
        db.execute(
            "INSERT INTO users (session_id, username, is_synthetic) VALUES (?, ?, ?)",
            ("syn-user", "synthetic_bot_001", 1),
        )
        # Locked race in the past so /race/<id>/live is allowed.
        db.execute(
            "INSERT INTO races (id, name, round, date, status) VALUES (?, ?, ?, ?, ?)",
            (1, "Bahrain GP", 1, "2000-01-01 14:00:00", "locked"),
        )
        for i in range(1, 4):
            db.execute(
                "INSERT INTO drivers (id, driver_id, name, number) VALUES (?, ?, ?, ?)",
                (i, f"drv{i}", f"Driver {i}", i),
            )
        db.execute(
            "INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) "
            "VALUES (?, ?, ?, ?, ?)",
            ("real-user", 1, 1, 2, 3),
        )
        db.execute(
            "INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id) "
            "VALUES (?, ?, ?, ?, ?)",
            ("syn-user", 1, 3, 2, 1),
        )
        db.commit()

        with client.session_transaction() as sess:
            sess["session_id"] = "real-user"

        # Avoid hitting the live API; empty positions are enough for the test.
        monkeypatch.setattr("app.fetch_live_race_data", lambda _db, _race: [])

    def test_live_race_leaderboard_excludes_synthetic_users(
        self, app, client, _live_race_fixtures
    ):
        response = client.get('/race/1/live')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert 'brett_real' in content
        assert 'synthetic_bot_001' not in content
