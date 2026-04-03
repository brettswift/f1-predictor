"""Unit tests for race manager state machine (race_manager.py)."""

import pytest
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

# Set test database BEFORE importing race_manager
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'
os.environ['F1_SEASON'] = '2026'

# Add cron/ to path so we can import race_manager
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))

# Import after setting env vars
import race_manager as rm


class MockDB:
    """In-memory database for testing race_manager functions."""
    
    def __init__(self):
        import sqlite3
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self._setup_schema()
    
    def _setup_schema(self):
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE races (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                round INTEGER NOT NULL,
                date TEXT NOT NULL,
                status TEXT DEFAULT 'open'
            )
        ''')
        c.execute('''
            CREATE TABLE race_stages (
                race_id INTEGER PRIMARY KEY,
                stage TEXT NOT NULL,
                entered_at TEXT NOT NULL,
                last_poll_at TEXT,
                poll_count INTEGER DEFAULT 0
            )
        ''')
        c.execute('''
            CREATE TABLE drivers (
                id INTEGER PRIMARY KEY,
                driver_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                number INTEGER NOT NULL,
                code TEXT,
                nationality TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                race_id INTEGER NOT NULL,
                p1_driver_id INTEGER NOT NULL,
                p2_driver_id INTEGER NOT NULL,
                p3_driver_id INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE results (
                race_id INTEGER PRIMARY KEY,
                p1_driver_id INTEGER NOT NULL,
                p2_driver_id INTEGER NOT NULL,
                p3_driver_id INTEGER NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE scores (
                id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                race_id INTEGER NOT NULL,
                points INTEGER NOT NULL,
                UNIQUE(user_id, race_id)
            )
        ''')
        c.execute('''
            CREATE TABLE users (
                session_id TEXT PRIMARY KEY,
                username TEXT NOT NULL
            )
        ''')
        self.conn.commit()
    
    def execute(self, sql, args=()):
        return self.conn.execute(sql, args)
    
    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.conn.close()


class TestRaceManagerStateMachine:
    """Test cases for F1-CJ-2: Test race manager state machine."""

    @pytest.fixture
    def db(self):
        """Create in-memory database with schema."""
        database = MockDB()
        rm.ensure_stage_table(database)
        
        # Insert test drivers
        database.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1, 'max_verstappen', 'Max Verstappen', 1, 'VER', 'NED'))
        database.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (2, 'lando_norris', 'Lando Norris', 4, 'NOR', 'GBR'))
        database.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (3, 'charlie_leclerc', 'Charles Leclerc', 16, 'LEC', 'MON'))
        database.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (4, 'carlos_sainz', 'Carlos Sainz', 55, 'SAI', 'ESP'))
        database.commit()
        
        yield database
        database.close()
    
    def test_watching_to_locked_race_starts_within_6_min(self, db):
        """CJ-004: watching → locked when race starts within 6 minutes.
        
        Given a race is in 'watching' stage
        And the race starts in 5 minutes
        When promote_to_locked is called
        Then the race should transition to 'locked' stage
        And race status should be 'locked'
        """
        race_time = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = race_time - timedelta(minutes=5)  # 5 min before start
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (1, 'Test Grand Prix', 10, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'open'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, ?, ?)
        ''', (1, 'watching', (race_time - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')))
        db.commit()
        
        rm.promote_to_locked(db, now)
        
        stage = db.execute('SELECT stage FROM race_stages WHERE race_id = 1').fetchone()
        assert stage['stage'] == 'locked', f"Expected 'locked', got '{stage['stage']}'"
        
        race_status = db.execute('SELECT status FROM races WHERE id = 1').fetchone()
        assert race_status['status'] == 'locked'
    
    def test_watching_stays_watching_race_more_than_6_min_away(self, db):
        """CJ-004 variant: watching stays watching when race > 6 min away.
        
        Given a race is in 'watching' stage
        And the race starts in 10 minutes
        When promote_to_locked is called
        Then the race should stay in 'watching' stage
        """
        race_time = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = race_time - timedelta(minutes=10)  # 10 min before start
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (2, 'Test Grand Prix 2', 11, race_time.strftime('%Y-%m-%d %H:%M:%S'), 'open'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, ?, ?)
        ''', (2, 'watching', (race_time - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')))
        db.commit()
        
        rm.promote_to_locked(db, now)
        
        stage = db.execute('SELECT stage FROM race_stages WHERE race_id = 2').fetchone()
        assert stage['stage'] == 'watching', f"Expected 'watching', got '{stage['stage']}'"
    
    def test_locked_to_polling_after_90_min(self, db):
        """CJ-005: locked → polling after 90 minutes since lock.
        
        Given a race is in 'locked' stage
        And 90 minutes have passed since it was locked
        When promote_to_polling is called
        Then the race should transition to 'polling' stage
        """
        locked_time = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = locked_time + timedelta(hours=1, minutes=30)  # exactly 90 min later
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (3, 'Test Grand Prix 3', 12, locked_time.strftime('%Y-%m-%d %H:%M:%S'), 'locked'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, ?, ?)
        ''', (3, 'locked', locked_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        db.commit()
        
        rm.promote_to_polling(db, now)
        
        stage = db.execute('SELECT stage FROM race_stages WHERE race_id = 3').fetchone()
        assert stage['stage'] == 'polling', f"Expected 'polling', got '{stage['stage']}'"
    
    def test_locked_stays_locked_before_90_min(self, db):
        """CJ-005 variant: locked stays locked before 90 min.
        
        Given a race is in 'locked' stage
        And only 60 minutes have passed
        When promote_to_polling is called
        Then the race should stay in 'locked' stage
        """
        locked_time = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = locked_time + timedelta(hours=1)  # only 60 min later
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (4, 'Test Grand Prix 4', 13, locked_time.strftime('%Y-%m-%d %H:%M:%S'), 'locked'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at)
            VALUES (?, ?, ?)
        ''', (4, 'locked', locked_time.strftime('%Y-%m-%dT%H:%M:%SZ')))
        db.commit()
        
        rm.promote_to_polling(db, now)
        
        stage = db.execute('SELECT stage FROM race_stages WHERE race_id = 4').fetchone()
        assert stage['stage'] == 'locked', f"Expected 'locked', got '{stage['stage']}'"
    
    def test_polling_to_completed_when_api_returns_results(self, db, monkeypatch):
        """CJ-006: polling → completed when API returns results.
        
        Given a race is in 'polling' stage
        And the F1 API returns race results
        When poll_for_results is called
        Then the race should transition to 'completed' stage
        And scores should be calculated for all predictions
        """
        polling_started = datetime(2026, 6, 15, 15, 30, 0, tzinfo=timezone.utc)
        now = polling_started + timedelta(minutes=5)
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (5, 'Test Grand Prix 5', 14, '2026-06-15 14:00:00', 'locked'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (5, 'polling', polling_started.strftime('%Y-%m-%dT%H:%M:%SZ'), None, 0))
        
        # Add user and predictions
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('user-abc-123', 'testuser'))
        
        # User predicted P1=verstappen(1), P2=norris(2), P3=leclerc(3)
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('user-abc-123', 5, 1, 2, 3))
        db.commit()
        
        # Mock API response
        def mock_fetch_podium(season, round_num):
            return {
                'p1': {'driver_name': 'Max Verstappen', 'driver_code': 'VER', 'constructor': 'Red Bull'},
                'p2': {'driver_name': 'Lando Norris', 'driver_code': 'NOR', 'constructor': 'McLaren'},
                'p3': {'driver_name': 'Charles Leclerc', 'driver_code': 'LEC', 'constructor': 'Ferrari'},
            }
        
        monkeypatch.setattr('race_manager._fetch_podium', mock_fetch_podium)
        
        rm.poll_for_results(db, now)
        
        stage = db.execute('SELECT stage FROM race_stages WHERE race_id = 5').fetchone()
        assert stage['stage'] == 'completed', f"Expected 'completed', got '{stage['stage']}'"
        
        race = db.execute('SELECT status FROM races WHERE id = 5').fetchone()
        assert race['status'] == 'completed'
        
        # Verify results were saved
        results = db.execute('SELECT * FROM results WHERE race_id = 5').fetchone()
        assert results is not None
        assert results['p1_driver_id'] == 1  # Max Verstappen
        assert results['p2_driver_id'] == 2  # Lando Norris
        assert results['p3_driver_id'] == 3  # Charles Leclerc
        
        # Verify scores were calculated
        score = db.execute('SELECT * FROM scores WHERE user_id = ?', ('user-abc-123',)).fetchone()
        assert score is not None
        # All 3 correct = 10 + 6 + 4 = 20 points
        assert score['points'] == 20, f"Expected 20 points (all 3 correct), got {score['points']}"
    
    def test_polling_stays_polling_when_no_results_yet(self, db, monkeypatch):
        """CJ-006 variant: polling stays polling when API has no results yet.
        
        Given a race is in 'polling' stage
        And the F1 API returns no results (race not finished)
        When poll_for_results is called
        Then the race should stay in 'polling' stage
        And poll_count should increment
        """
        polling_started = datetime(2026, 6, 15, 15, 30, 0, tzinfo=timezone.utc)
        now = polling_started + timedelta(minutes=5)
        
        db.execute('''
            INSERT INTO races (id, name, round, date, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (6, 'Test Grand Prix 6', 15, '2026-06-15 14:00:00', 'locked'))
        
        db.execute('''
            INSERT INTO race_stages (race_id, stage, entered_at, last_poll_at, poll_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (6, 'polling', polling_started.strftime('%Y-%m-%dT%H:%M:%SZ'), None, 0))
        db.commit()
        
        # Mock API returning no results
        def mock_fetch_podium(season, round_num):
            return None
        
        monkeypatch.setattr('race_manager._fetch_podium', mock_fetch_podium)
        
        rm.poll_for_results(db, now)
        
        stage = db.execute('SELECT stage, poll_count FROM race_stages WHERE race_id = 6').fetchone()
        assert stage['stage'] == 'polling', f"Expected 'polling', got '{stage['stage']}'"
        assert stage['poll_count'] == 1, f"Expected poll_count=1, got {stage['poll_count']}"
