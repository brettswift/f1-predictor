"""Unit tests for results edge cases (F1-RI-3: Test results edge cases).

T-IDs: RI-007, RI-008, RI-009, RI-010

AC:
- Results ingested for locked races
- Scores calculated
- No data = retry
- Only processes locked races
"""

import pytest
import os
import sys
import responses
import sqlite3
from datetime import datetime, timezone, timedelta

# Add cron/ to path so we can import race_manager
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))


def create_test_db(db_path):
    """Create a test database with the full schema."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # Users table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Drivers table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY,
            driver_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            team TEXT,
            number INTEGER NOT NULL,
            code TEXT,
            nationality TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Races table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            round INTEGER NOT NULL,
            date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Predictions table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(session_id),
            FOREIGN KEY (race_id) REFERENCES races(id),
            UNIQUE(user_id, race_id)
        )
    ''')
    
    # Results table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS results (
            race_id INTEGER PRIMARY KEY,
            p1_driver_id INTEGER NOT NULL,
            p2_driver_id INTEGER NOT NULL,
            p3_driver_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (race_id) REFERENCES races(id)
        )
    ''')
    
    # Scores table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            race_id INTEGER NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(session_id),
            FOREIGN KEY (race_id) REFERENCES races(id),
            UNIQUE(user_id, race_id)
        )
    ''')
    
    # Race stages table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS race_stages (
            race_id INTEGER PRIMARY KEY,
            stage TEXT NOT NULL,
            entered_at TEXT NOT NULL,
            last_poll_at TEXT,
            poll_count INTEGER DEFAULT 0,
            FOREIGN KEY (race_id) REFERENCES races(id)
        )
    ''')
    
    conn.commit()
    return conn


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary file-based database for testing.
    
    IMPORTANT: Don't reload the app module because it triggers init_db()
    at module level which fetches drivers from the API.
    """
    db_path = tmp_path / "test.db"
    os.environ['DATABASE_PATH'] = str(db_path)
    
    # Create fresh database with schema
    conn = create_test_db(str(db_path))
    
    # DON'T import or reload app - just use the database directly
    # The race_manager module will use DATABASE_PATH env var when needed
    
    yield conn
    
    conn.close()


@pytest.fixture
def rm_with_db(test_db):
    """Get race_manager module functions that work with our test database.
    
    We monkeypatch get_db to return our test database connection.
    """
    import race_manager as rm
    import importlib
    
    # Monkeypatch get_db to return our test connection
    original_get_db = rm.get_db
    rm.get_db = lambda: test_db
    
    yield rm
    
    # Restore original
    rm.get_db = original_get_db


class TestResultsIngestedForLockedRaces:
    """Test cases for RI-008: Results ingested for locked races."""

    @responses.activate
    def test_ri_008_podium_data_ingested_correctly(self, test_db, rm_with_db):
        """RI-008: Results ingested for locked races - API returns correct podium data.
        
        Given a race with status 'locked' is being polled
        When the Ergast API returns valid podium results
        Then those results are correctly ingested into the database
        """
        rm = rm_with_db
        db = test_db
        
        # Setup: locked race with race_stages entry in polling stage
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (201, 'Test Grand Prix', 5, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Add drivers for the podium
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (501, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (502, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (503, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        # Setup race_stages for polling
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (201, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock the API response
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/5/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "5",
                            "raceName": "Test Grand Prix",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        # Execute polling
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify results were ingested
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (201,)).fetchone()
        assert result is not None, "Results should be ingested for locked race"
        assert result['p1_driver_id'] == 501
        assert result['p2_driver_id'] == 502
        assert result['p3_driver_id'] == 503
        
        # Verify race status updated
        race = db.execute('SELECT * FROM races WHERE id = ?', (201,)).fetchone()
        assert race['status'] == 'completed', "Race should be marked as completed"

    @responses.activate
    def test_ri_008_ignores_non_podium_results(self, test_db, rm_with_db):
        """RI-008: Only top 3 drivers are ingested as podium results.
        
        Given the API returns more than 3 drivers
        When results are ingested
        Then only the top 3 (podium) are stored
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (202, 'Full Results Race', 6, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Add drivers for the podium
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (511, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (512, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (513, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (202, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock API with 10 drivers (full grid)
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/6/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "6",
                            "raceName": "Full Results Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                                {"position": "4", "Driver": {"code": "PIA", "givenName": "Oscar", "familyName": "Piastri"}, "Constructor": {"name": "McLaren"}},
                                {"position": "5", "Driver": {"code": "RUS", "givenName": "George", "familyName": "Russell"}, "Constructor": {"name": "Mercedes"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (202,)).fetchone()
        assert result is not None, "Results should be ingested"
        assert result['p1_driver_id'] == 511
        assert result['p2_driver_id'] == 512
        assert result['p3_driver_id'] == 513


class TestScoresCalculated:
    """Test cases for RI-009: Scores calculated correctly."""

    @responses.activate
    def test_ri_009_perfect_prediction_scores_20(self, test_db, rm_with_db):
        """RI-009: Perfect prediction scores 20 points.
        
        Given a user has made a perfect prediction for a race
        When results are ingested
        Then the user should receive 20 points (10+6+4)
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (301, 'Score Test Race', 7, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Add drivers
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (601, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (602, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (603, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        # Add user with perfect prediction
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('perfect-user', 'perfectfan'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('perfect-user', 301, 601, 602, 603))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (301, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/7/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "7",
                            "raceName": "Score Test Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('perfect-user', 301)).fetchone()
        assert score is not None, "Score should be calculated"
        assert score['points'] == 20, f"Perfect prediction should score 20, got {score['points']}"

    @responses.activate
    def test_ri_009_partial_prediction_scores_correctly(self, test_db, rm_with_db):
        """RI-009: Partial prediction scores correctly based on correctness.
        
        Given a user predicted P1 and P3 correctly but P2 wrong
        When results are ingested
        Then the user should receive 14 points (10+0+4)
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (302, 'Partial Score Race', 8, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Drivers: VER=1, NOR=2, LEC=3 in race results
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (611, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (612, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (613, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        # User predicts: VER P1, LEC P2, NOR P3 (P1 and P3 correct positions, P2 wrong)
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('partial-user', 'partialfan'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('partial-user', 302, 611, 613, 612))  # VER, LEC, NOR
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (302, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Race results: P1=VER, P2=NOR, P3=LEC
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/8/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "8",
                            "raceName": "Partial Score Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('partial-user', 302)).fetchone()
        assert score is not None, "Score should be calculated"
        # P1 correct: +10
        # P2 predicted LEC, result NOR - LEC is on podium (P3) so +1
        # P3 predicted NOR, result LEC - NOR is on podium (P2) so +1
        # Total: 10 + 1 + 1 = 12
        assert score['points'] == 12, f"P1 correct + two on podium wrong position should score 12, got {score['points']}"

    @responses.activate
    def test_ri_009_all_wrong_prediction_scores_0(self, test_db, rm_with_db):
        """RI-009: All wrong prediction scores 0 points.
        
        Given a user predicted all drivers wrong
        When results are ingested
        Then the user should receive 0 points
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (303, 'Zero Score Race', 9, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (621, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (622, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (623, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        # User predicts: ALO, RUS, SAI (all wrong)
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (624, 'alonso', 'Fernando Alonso', 14, 'ALO'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (625, 'russell', 'George Russell', 63, 'RUS'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (626, 'sainz', 'Carlos Sainz', 55, 'SAI'))
        
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('zerouser', 'zerofan'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('zerouser', 303, 624, 625, 626))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (303, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/9/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "9",
                            "raceName": "Zero Score Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        score = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('zerouser', 303)).fetchone()
        assert score is not None, "Score should be calculated"
        assert score['points'] == 0, f"All wrong should score 0, got {score['points']}"

    @responses.activate
    def test_ri_009_multiple_users_scores_calculated(self, test_db, rm_with_db):
        """RI-009: Multiple users all get their scores calculated.
        
        Given multiple users with different predictions for the same race
        When results are ingested
        Then each user should receive their correct score
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (304, 'Multi User Race', 10, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (631, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (632, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (633, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        # User 1: Perfect prediction (20 points)
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('user1', 'userone'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('user1', 304, 631, 632, 633))
        
        # User 2: P1 correct only (10 points)
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('user2', 'usertwo'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('user2', 304, 631, 633, 632))
        
        # User 3: All wrong (0 points)
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (634, 'alonso', 'Fernando Alonso', 14, 'ALO'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (635, 'hamilton', 'Lewis Hamilton', 44, 'HAM'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (636, 'landonorris', 'Oscar Piastri', 81, 'PIA'))
        
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('user3', 'userthree'))
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('user3', 304, 634, 635, 636))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (304, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/10/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "10",
                            "raceName": "Multi User Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        score1 = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('user1', 304)).fetchone()
        score2 = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('user2', 304)).fetchone()
        score3 = db.execute('SELECT * FROM scores WHERE user_id = ? AND race_id = ?', ('user3', 304)).fetchone()
        
        assert score1['points'] == 20, f"User1 (perfect) should score 20, got {score1['points']}"
        # User2: P1=VER, P2=LEC, P3=NOR vs actual P1=VER, P2=NOR, P3=LEC
        # P1 correct: +10
        # P2 predicted LEC, result NOR - LEC is on podium wrong: +1
        # P3 predicted NOR, result LEC - NOR is on podium wrong: +1
        # Total: 10 + 1 + 1 = 12
        assert score2['points'] == 12, f"User2 should score 12 (P1 + two on podium wrong), got {score2['points']}"
        assert score3['points'] == 0, f"User3 (all wrong) should score 0, got {score3['points']}"


class TestNoDataRetry:
    """Test cases for RI-010: No data = retry next run."""

    @responses.activate
    def test_ri_010_no_results_returns_none_and_retries(self, test_db, rm_with_db):
        """RI-010: No data from API = retry next run.
        
        Given a race is in polling stage
        When the API returns no results (empty or incomplete)
        Then the system should continue polling (not give up)
        And results should be retried on next run
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (401, 'Retry Test Race', 11, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (401, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock API returning empty results
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/11/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": []  # No results yet
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify still in polling stage (not completed)
        stage = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (401,)).fetchone()
        assert stage['stage'] == 'polling', "Should still be polling when no results"
        assert stage['poll_count'] == 1, "Poll count should increment"
        
        # Verify no results stored
        result = db.execute('SELECT * FROM results WHERE race_id = ?', (401,)).fetchone()
        assert result is None, "No results should be stored when API returns empty"

    @responses.activate
    def test_ri_010_incomplete_results_triggers_retry(self, test_db, rm_with_db):
        """RI-010: Incomplete podium data triggers retry.
        
        Given a race is in polling stage
        When the API returns only 2 drivers (race not complete)
        Then the system should continue polling
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (402, 'Incomplete Test Race', 12, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (402, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock API returning only 2 results (race not finished)
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/12/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "12",
                            "raceName": "Incomplete Test Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify still in polling stage
        stage = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (402,)).fetchone()
        assert stage['stage'] == 'polling', "Should still be polling when results incomplete"
        assert stage['poll_count'] == 1, "Poll count should increment"

    @responses.activate
    def test_ri_010_api_error_triggers_retry(self, test_db, rm_with_db):
        """RI-010: API error = retry next run.
        
        Given a race is in polling stage
        When the API returns an error
        Then the system should continue polling
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (403, 'Error Test Race', 13, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (403, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock API returning error status
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/13/results.json',
            json={"error": "Service unavailable"},
            status=503
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify still in polling stage
        stage = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (403,)).fetchone()
        assert stage['stage'] == 'polling', "Should still be polling when API error"


class TestOnlyProcessesLockedRaces:
    """Test cases for RI-007: Only processes locked races."""

    def test_ri_007_does_not_process_open_races(self, test_db, rm_with_db):
        """RI-007: Only processes locked races - open races ignored.
        
        Given a race with status 'open' is in watching stage
        When poll_for_results runs
        Then it should not process that race
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        
        # Open race (not locked)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'open')
        ''', (501, 'Open Race', 14, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Race in watching stage (not polling)
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'watching', ?, NULL, 0)
        ''', (501, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify race still in watching (not processed)
        stage = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (501,)).fetchone()
        assert stage['stage'] == 'watching', "Open race should stay in watching, not be polled"

    def test_ri_007_does_not_process_completed_races(self, test_db, rm_with_db):
        """RI-007: Only processes locked races - completed races ignored.
        
        Given a race with status 'completed'
        When poll_for_results runs
        Then it should not process that race again
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        
        # Completed race
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'completed')
        ''', (502, 'Completed Race', 15, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        # Race in completed stage
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'completed', ?, NULL, 0)
        ''', (502, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify poll_count still 0 (not polled)
        stage = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (502,)).fetchone()
        assert stage['poll_count'] == 0, "Completed race should not be polled"

    @responses.activate
    def test_ri_007_only_polls_races_in_polling_stage(self, test_db, rm_with_db):
        """RI-007: Only races in 'polling' stage are polled.
        
        Given multiple races in different stages
        When poll_for_results runs
        Then only races in 'polling' stage are processed
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        
        # Race 1: watching stage (should NOT be polled)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'open')
        ''', (511, 'Watching Race', 16, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'watching', ?, NULL, 0)
        ''', (511, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        # Race 2: locked stage (should NOT be polled)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (512, 'Locked Race', 17, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'locked', ?, NULL, 0)
        ''', (512, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        # Race 3: polling stage (SHOULD be polled)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (513, 'Polling Race', 18, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (701, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (702, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (703, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (513, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        # Race 4: completed stage (should NOT be polled)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'completed')
        ''', (514, 'Completed Race 2', 19, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'completed', ?, NULL, 0)
        ''', (514, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        # Mock the API for the polling race
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/18/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "18",
                            "raceName": "Polling Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        # Verify only polling race was processed
        stage1 = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (511,)).fetchone()
        stage2 = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (512,)).fetchone()
        stage3 = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (513,)).fetchone()
        stage4 = db.execute('SELECT * FROM race_stages WHERE race_id = ?', (514,)).fetchone()
        
        assert stage1['poll_count'] == 0, "Watching race should not be polled"
        assert stage2['poll_count'] == 0, "Locked race should not be polled"
        assert stage3['poll_count'] == 1, "Polling race should be polled"
        assert stage4['poll_count'] == 0, "Completed race should not be polled"
        
        # Verify the polling race completed
        assert stage3['stage'] == 'completed', "Polling race should complete"


class TestRaceStatusTransition:
    """Test race status transitions when results are ingested."""

    @responses.activate
    def test_race_status_transitions_to_completed(self, test_db, rm_with_db):
        """Race transitions from locked to completed after results ingested.
        
        Given a locked race is being polled
        When valid podium results are received
        Then the race status should change to 'completed'
        """
        rm = rm_with_db
        db = test_db
        
        race_time = datetime(2026, 3, 28, 13, 0, 0, tzinfo=timezone.utc)
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, 'locked')
        ''', (601, 'Status Transition Race', 20, race_time.strftime('%Y-%m-%d %H:%M:%S')))
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (801, 'verstappen', 'Max Verstappen', 1, 'VER'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (802, 'norris', 'Lando Norris', 4, 'NOR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code)
            VALUES (?, ?, ?, ?, ?)
        ''', (803, 'leclerc', 'Charles Leclerc', 16, 'LEC'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, 'polling', ?, NULL, 0)
        ''', (601, race_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        db.commit()
        
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/20/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "20",
                            "raceName": "Status Transition Race",
                            "Results": [
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen"}, "Constructor": {"name": "Red Bull"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        now = race_time + timedelta(hours=2)
        rm.poll_for_results(db, now)
        
        race = db.execute('SELECT * FROM races WHERE id = ?', (601,)).fetchone()
        assert race['status'] == 'completed', f"Race should be completed, got {race['status']}"
