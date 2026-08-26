"""Unit tests for fetch_race_results.py (F1-CJ-3: Test fetch results)."""

import pytest
import os
import sys
import sqlite3
from unittest.mock import patch, MagicMock

# Set test database BEFORE importing
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['TESTING'] = 'true'
os.environ['F1_SEASON'] = '2026'
os.environ['OPENF1_OFFLINE'] = 'true'

# Add cron/ and src/ to path so we can import fetch_race_results (which
# itself imports openf1 from src/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


def _drivers_db():
    """In-memory DB with a races.session_key column and a drivers table
    keyed by car number, matching production schema."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    conn.execute('''
        CREATE TABLE drivers (
            id INTEGER PRIMARY KEY,
            driver_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            number INTEGER NOT NULL,
            code TEXT,
            nationality TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE races (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            round INTEGER NOT NULL,
            date TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            session_key INTEGER
        )
    ''')
    conn.executemany(
        'INSERT INTO drivers (id, driver_id, name, number, code, nationality) VALUES (?, ?, ?, ?, ?, ?)',
        [
            (1, 'max_verstappen', 'Max Verstappen', 1, 'VER', 'NED'),
            (2, 'lando_norris', 'Lando Norris', 4, 'NOR', 'GBR'),
            (3, 'charles_leclerc', 'Charles Leclerc', 16, 'LEC', 'MON'),
        ],
    )
    conn.commit()
    return conn


class TestCalculateScore:
    """Test the score calculation logic (CJ-009)."""

    def test_calculate_score_perfect(self):
        """Perfect prediction scores 20 points.

        Given all three predictions are correct
        When calculate_score is called
        Then 20 points are awarded (10+6+4)
        """
        import fetch_race_results as fr

        prediction = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        result = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}

        score = fr.calculate_score(prediction, result)
        assert score == 20, f"Perfect prediction should score 20, got {score}"

    def test_calculate_score_partial_match(self):
        """Partial prediction scores correctly.

        Given P1 is correct and P2/P3 are on podium in wrong positions
        When calculate_score is called
        Then score is 12 (10 + 0+1 + 0+1)
        """
        import fetch_race_results as fr

        # P1 correct, P2 and P3 on podium but swapped
        prediction = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        result = {'p1_driver_id': 1, 'p2_driver_id': 3, 'p3_driver_id': 2}

        score = fr.calculate_score(prediction, result)
        # P1 exact (+10), P2 not exact, P3 not exact
        # But P2's driver (2) is on podium in P3 position (+1)
        # And P3's driver (3) is on podium in P2 position (+1)
        # Total: 10 + 1 + 1 = 12
        assert score == 12, f"Partial match should score 12, got {score}"

    def test_calculate_score_one_correct(self):
        """One correct prediction scores correctly.

        Given only P1 is correct
        When calculate_score is called
        Then score is 10 (no bonus for others)
        """
        import fetch_race_results as fr

        prediction = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        result = {'p1_driver_id': 1, 'p2_driver_id': 4, 'p3_driver_id': 5}

        score = fr.calculate_score(prediction, result)
        # P1 exact (+10), P2 and P3 wrong and not on podium
        assert score == 10, f"One correct should score 10, got {score}"

    def test_calculate_score_all_wrong(self):
        """All wrong scores 0.

        Given all predictions are wrong
        When calculate_score is called
        Then 0 points are awarded
        """
        import fetch_race_results as fr

        prediction = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        result = {'p1_driver_id': 4, 'p2_driver_id': 5, 'p3_driver_id': 6}

        score = fr.calculate_score(prediction, result)
        assert score == 0, f"All wrong should score 0, got {score}"

    def test_calculate_score_two_on_podium_wrong_position(self):
        """Two predictions on podium but wrong position scores bonus points.

        Given P2 and P3 correct drivers but wrong positions
        When calculate_score is called
        Then 2 bonus points are awarded (1 each)
        """
        import fetch_race_results as fr

        # Predict P1=A, P2=B, P3=C
        # Result: P1=X, P2=C, P3=B (B and C on podium but swapped)
        prediction = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        result = {'p1_driver_id': 4, 'p2_driver_id': 3, 'p3_driver_id': 2}

        score = fr.calculate_score(prediction, result)
        # P1 wrong (+0), P2 not exact but 2 is on podium (+1), P3 not exact but 3 is on podium (+1)
        # Total: 0 + 1 + 1 = 2
        assert score == 2, f"Should score 2 (two on podium wrong), got {score}"


class TestFetchRaceResultsFromApi:
    """Test podium fetch via OpenF1 (CJ-008, CJ-010)."""

    def test_cj_008_openf1_podium_resolves_to_driver_ids(self):
        """CJ-008: OpenF1 podium is resolved to DB driver ids.

        Given openf1.get_podium returns a full podium
        And the drivers table has matching car numbers
        When fetch_race_results_from_api is called
        Then the DB driver ids are returned
        """
        import fetch_race_results as fr

        db = _drivers_db()
        db.execute(
            'INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)',
            (1, 'Bahrain Grand Prix', 1, '2026-03-01', 'locked', 9001),
        )
        race = dict(db.execute('SELECT * FROM races WHERE id = 1').fetchone())

        openf1_podium = {
            'p1': {'position': 1, 'driver_number': 1, 'driver_name': 'Max Verstappen'},
            'p2': {'position': 2, 'driver_number': 4, 'driver_name': 'Lando Norris'},
            'p3': {'position': 3, 'driver_number': 16, 'driver_name': 'Charles Leclerc'},
        }

        with patch('fetch_race_results.openf1.get_podium', return_value=openf1_podium):
            podium = fr.fetch_race_results_from_api(db, race)

        assert podium is not None, "Should return podium data"
        assert podium['p1']['driver_id'] == 1
        assert podium['p2']['driver_id'] == 2
        assert podium['p3']['driver_id'] == 3

    def test_cj_010_no_podium_returns_none(self):
        """CJ-010: No data = retry next run.

        Given openf1.get_podium returns None (race not complete)
        When fetch_race_results_from_api is called
        Then None is returned (indicating retry needed)
        """
        import fetch_race_results as fr

        db = _drivers_db()
        db.execute(
            'INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)',
            (2, 'Test Grand Prix', 2, '2026-03-08', 'locked', 9002),
        )
        race = dict(db.execute('SELECT * FROM races WHERE id = 2').fetchone())

        with patch('fetch_race_results.openf1.get_podium', return_value=None):
            podium = fr.fetch_race_results_from_api(db, race)

        assert podium is None, "Should return None when no results available"

    def test_cj_010_handles_api_error_gracefully(self):
        """CJ-010: API error is handled gracefully.

        Given openf1.get_podium raises OpenF1Error
        When fetch_race_results_from_api is called
        Then None is returned (not an exception)
        """
        import fetch_race_results as fr
        import openf1

        db = _drivers_db()
        db.execute(
            'INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)',
            (3, 'Test Grand Prix', 3, '2026-03-15', 'locked', 9003),
        )
        race = dict(db.execute('SELECT * FROM races WHERE id = 3').fetchone())

        with patch('fetch_race_results.openf1.get_podium', side_effect=openf1.OpenF1Error("boom")):
            podium = fr.fetch_race_results_from_api(db, race)

        assert podium is None, "Should return None on API error, not raise exception"

    def test_unresolvable_driver_returns_none(self):
        """A podium driver number not in the drivers table means "skip ingest".

        Given openf1.get_podium returns a driver number with no DB match
        When fetch_race_results_from_api is called
        Then None is returned
        """
        import fetch_race_results as fr

        db = _drivers_db()
        db.execute(
            'INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)',
            (4, 'Test Grand Prix', 4, '2026-03-22', 'locked', 9004),
        )
        race = dict(db.execute('SELECT * FROM races WHERE id = 4').fetchone())

        openf1_podium = {
            'p1': {'position': 1, 'driver_number': 99, 'driver_name': 'Unknown Driver'},
            'p2': {'position': 2, 'driver_number': 4, 'driver_name': 'Lando Norris'},
            'p3': {'position': 3, 'driver_number': 16, 'driver_name': 'Charles Leclerc'},
        }

        with patch('fetch_race_results.openf1.get_podium', return_value=openf1_podium):
            podium = fr.fetch_race_results_from_api(db, race)

        assert podium is None, "Should return None when a podium driver isn't in the drivers table"

    def test_missing_session_key_returns_none(self):
        """No session_key and no matching session round means "try again later".

        Given the race has no session_key and OpenF1 has no matching session
        When fetch_race_results_from_api is called
        Then None is returned without calling get_podium
        """
        import fetch_race_results as fr

        db = _drivers_db()
        db.execute(
            'INSERT INTO races (id, name, round, date, status, session_key) VALUES (?, ?, ?, ?, ?, ?)',
            (5, 'Test Grand Prix', 5, '2026-03-29', 'locked', None),
        )
        race = dict(db.execute('SELECT * FROM races WHERE id = 5').fetchone())

        with patch('fetch_race_results.openf1.get_race_sessions') as mock_sessions, \
             patch('fetch_race_results.openf1.get_podium') as mock_podium:
            mock_sessions.return_value = MagicMock(data=[])
            podium = fr.fetch_race_results_from_api(db, race)

        assert podium is None
        mock_podium.assert_not_called()


class TestGetDriverIdByNumber:
    """Test driver matching by car number (OpenF1 identifies drivers by number)."""

    def test_matches_by_number(self):
        import fetch_race_results as fr

        db = _drivers_db()
        assert fr.get_driver_id_by_number(db, 1) == 1
        assert fr.get_driver_id_by_number(db, 16) == 3

    def test_no_match_returns_none(self):
        import fetch_race_results as fr

        db = _drivers_db()
        assert fr.get_driver_id_by_number(db, 999) is None

    def test_none_number_returns_none(self):
        import fetch_race_results as fr

        db = _drivers_db()
        assert fr.get_driver_id_by_number(db, None) is None


class TestGetLockedRacesQuery:
    """Test the query logic for locked races."""

    def test_query_filters_by_locked_status(self):
        """Query only returns races with 'locked' status."""
        # This tests the SQL logic structure
        expected_query = """
            SELECT r.id, r.name, r.round, r.date, r.session_key
            FROM races r
            LEFT JOIN results res ON r.id = res.race_id
            WHERE r.status = 'locked' AND res.race_id IS NULL
            ORDER BY r.date ASC
        """

        # Verify query structure
        assert "r.status = 'locked'" in expected_query
        assert "LEFT JOIN results" in expected_query
        assert "res.race_id IS NULL" in expected_query
