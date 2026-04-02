"""Unit tests for driver refresh functionality."""

import pytest
from datetime import datetime, timezone

from tests.utils.time_control import TimeController


class TestDriverRefresh:
    """Test cases for F1-CJ-1: Test driver refresh cron job."""

    def test_drivers_table_updated_from_api(self, app, monkeypatch):
        """CJ-001: Drivers table is updated from API after refresh.
        
        Given a driver exists in the database with certain attributes
        When refresh_drivers_from_api is called with new API data
        Then the drivers table should contain the new driver data
        """
        from app import get_db, refresh_drivers_from_api
        
        # Mock API response with new drivers
        def mock_fetch_drivers():
            return [
                {'driver_id': 'max_verstappen', 'name': 'Max Verstappen', 'number': 1, 'code': 'VER', 'nationality': 'NED', 'team': 'Red Bull'},
                {'driver_id': 'lando_norris', 'name': 'Lando Norris', 'number': 4, 'code': 'NOR', 'nationality': 'GBR', 'team': 'McLaren'},
            ]
        
        monkeypatch.setattr('app.fetch_drivers_from_api', mock_fetch_drivers)
        
        db = get_db()
        # Clear existing drivers to start fresh
        db.execute('DELETE FROM drivers')
        db.execute('DELETE FROM predictions')
        db.execute('DELETE FROM users')
        db.execute('DELETE FROM metadata')
        db.commit()
        
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1, 'old_driver', 'Old Driver', 99, 'OLD', 'USA'))
        db.commit()
        
        success, message = refresh_drivers_from_api(db)
        
        assert success is True
        drivers = db.execute('SELECT * FROM drivers ORDER BY id').fetchall()
        assert len(drivers) == 2
        assert drivers[0]['driver_id'] == 'max_verstappen'
        assert drivers[1]['driver_id'] == 'lando_norris'

    def test_predictions_remain_valid_after_remap(self, app, monkeypatch):
        """CJ-002: Predictions remain valid after driver ID remapping.
        
        Given a user has made predictions using old driver IDs
        When refresh_drivers_from_api is called and driver IDs are remapped
        Then the predictions should still reference the correct drivers
        """
        from app import get_db, refresh_drivers_from_api
        
        # Mock API response - returns drivers in SAME order as DB
        def mock_fetch_drivers():
            return [
                {'driver_id': 'max_verstappen', 'name': 'Max Verstappen', 'number': 1, 'code': 'VER', 'nationality': 'NED', 'team': 'Red Bull'},
                {'driver_id': 'lando_norris', 'name': 'Lando Norris', 'number': 4, 'code': 'NOR', 'nationality': 'GBR', 'team': 'McLaren'},
                {'driver_id': 'charlie_leclerc', 'name': 'Charles Leclerc', 'number': 16, 'code': 'LEC', 'nationality': 'MON', 'team': 'Ferrari'},
            ]
        
        monkeypatch.setattr('app.fetch_drivers_from_api', mock_fetch_drivers)
        
        db = get_db()
        # Clear existing data to start fresh
        db.execute('DELETE FROM predictions')
        db.execute('DELETE FROM users')
        db.execute('DELETE FROM drivers')
        db.execute('DELETE FROM metadata')
        db.commit()
        
        # Insert drivers with specific IDs
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1, 'max_verstappen', 'Max Verstappen', 1, 'VER', 'NED'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (2, 'lando_norris', 'Lando Norris', 4, 'NOR', 'GBR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (3, 'charlie_leclerc', 'Charles Leclerc', 16, 'LEC', 'MON'))
        db.commit()
        
        # Create a user and prediction
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('test-session-123', 'testuser'))
        
        # Create a race
        db.execute('''
            INSERT INTO races (name, round, date, status)
            VALUES (?, ?, ?, ?)
        ''', ('Test Grand Prix', 1, '2026-12-31 14:00:00', 'completed'))
        
        # Prediction with P1=verstappen(1), P2=norris(2), P3=leclerc(3)
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('test-session-123', 1, 1, 2, 3))
        db.commit()
        
        # Verify initial state
        pred_before = db.execute('SELECT * FROM predictions WHERE user_id = ?', ('test-session-123',)).fetchone()
        assert pred_before['p1_driver_id'] == 1
        assert pred_before['p2_driver_id'] == 2
        assert pred_before['p3_driver_id'] == 3
        
        # Refresh drivers - IDs get remapped (max_verstappen keeps ID 1, lando_norris gets ID 2, charlie_leclerc gets ID 3)
        success, message = refresh_drivers_from_api(db)
        assert success is True
        
        # Check that predictions still exist and reference valid driver IDs
        pred_after = db.execute('SELECT * FROM predictions WHERE user_id = ?', ('test-session-123',)).fetchone()
        assert pred_after is not None
        # The prediction should still reference the same drivers by their NEW IDs
        # max_verstappen was ID 1, still ID 1
        # lando_norris was ID 2, still ID 2  
        # charlie_leclerc was ID 3, still ID 3
        assert pred_after['p1_driver_id'] == 1
        assert pred_after['p2_driver_id'] == 2
        assert pred_after['p3_driver_id'] == 3

    def test_predictions_remain_valid_when_api_returns_different_order(self, app, monkeypatch):
        """CJ-002 variant: Predictions remapped when API returns drivers in different order.
        
        Given a user predicted P1=verstappen, P2=norris, P3=leclerc (IDs 1, 2, 3)
        When the API returns drivers in a DIFFERENT order (norris first)
        Then predictions should be remapped to still point to correct drivers.
        """
        from app import get_db, refresh_drivers_from_api
        
        # Mock API response - returns drivers in DIFFERENT order than DB
        def mock_fetch_drivers():
            return [
                # norris comes FIRST in API response, so gets new ID 1
                {'driver_id': 'lando_norris', 'name': 'Lando Norris', 'number': 4, 'code': 'NOR', 'nationality': 'GBR', 'team': 'McLaren'},
                # verstappen comes SECOND, gets new ID 2
                {'driver_id': 'max_verstappen', 'name': 'Max Verstappen', 'number': 1, 'code': 'VER', 'nationality': 'NED', 'team': 'Red Bull'},
                # leclerc comes THIRD, gets new ID 3
                {'driver_id': 'charlie_leclerc', 'name': 'Charles Leclerc', 'number': 16, 'code': 'LEC', 'nationality': 'MON', 'team': 'Ferrari'},
            ]
        
        monkeypatch.setattr('app.fetch_drivers_from_api', mock_fetch_drivers)
        
        db = get_db()
        # Clear existing data
        db.execute('DELETE FROM predictions')
        db.execute('DELETE FROM users')
        db.execute('DELETE FROM drivers')
        db.execute('DELETE FROM metadata')
        db.commit()
        
        # Insert drivers in original order: verstappen=1, norris=2, leclerc=3
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1, 'max_verstappen', 'Max Verstappen', 1, 'VER', 'NED'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (2, 'lando_norris', 'Lando Norris', 4, 'NOR', 'GBR'))
        db.execute('''
            INSERT INTO drivers (id, driver_id, name, number, code, nationality)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (3, 'charlie_leclerc', 'Charles Leclerc', 16, 'LEC', 'MON'))
        db.commit()
        
        # Create a user and prediction: P1=verstappen(1), P2=norris(2), P3=leclerc(3)
        db.execute('''
            INSERT INTO users (session_id, username)
            VALUES (?, ?)
        ''', ('test-session-456', 'testuser2'))
        
        db.execute('''
            INSERT INTO races (name, round, date, status)
            VALUES (?, ?, ?, ?)
        ''', ('Test Grand Prix', 1, '2026-12-31 14:00:00', 'completed'))
        
        db.execute('''
            INSERT INTO predictions (user_id, race_id, p1_driver_id, p2_driver_id, p3_driver_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('test-session-456', 1, 1, 2, 3))  # P1=verstappen, P2=norris, P3=leclerc
        db.commit()
        
        # Verify initial state
        pred_before = db.execute('SELECT * FROM predictions WHERE user_id = ?', ('test-session-456',)).fetchone()
        assert pred_before['p1_driver_id'] == 1  # verstappen
        assert pred_before['p2_driver_id'] == 2  # norris
        assert pred_before['p3_driver_id'] == 3  # leclerc
        
        # Refresh drivers - API returns in different order: norris=1, verstappen=2, leclerc=3
        success, message = refresh_drivers_from_api(db)
        assert success is True
        
        # Check the new driver IDs after refresh
        drivers_after = {d['driver_id']: d['id'] for d in db.execute('SELECT driver_id, id FROM drivers').fetchall()}
        assert drivers_after['lando_norris'] == 1
        assert drivers_after['max_verstappen'] == 2
        assert drivers_after['charlie_leclerc'] == 3
        
        # Predictions should be remapped: 
        # P1 was verstappen(ID=1), now should be verstappen(ID=2)
        # P2 was norris(ID=2), now should be norris(ID=1)
        # P3 was leclerc(ID=3), now should be leclerc(ID=3)
        pred_after = db.execute('SELECT * FROM predictions WHERE user_id = ?', ('test-session-456',)).fetchone()
        assert pred_after['p1_driver_id'] == 2, f"P1 should be verstappen(ID=2), got {pred_after['p1_driver_id']}"
        assert pred_after['p2_driver_id'] == 1, f"P2 should be norris(ID=1), got {pred_after['p2_driver_id']}"
        assert pred_after['p3_driver_id'] == 3, f"P3 should be leclerc(ID=3), got {pred_after['p3_driver_id']}"

    def test_metadata_table_updated_with_timestamp(self, app, monkeypatch):
        """CJ-003: Metadata table is updated with timestamp after refresh.
        
        Given the metadata table may or may not have a refresh timestamp
        When refresh_drivers_from_api is called
        Then the metadata table should contain the current timestamp
        """
        from app import get_db, refresh_drivers_from_api
        
        def mock_fetch_drivers():
            return [
                {'driver_id': 'test_driver', 'name': 'Test Driver', 'number': 99, 'code': 'TST', 'nationality': 'USA', 'team': None},
            ]
        
        monkeypatch.setattr('app.fetch_drivers_from_api', mock_fetch_drivers)
        
        db = get_db()
        # Clear metadata and drivers
        db.execute('DELETE FROM metadata')
        db.execute('DELETE FROM drivers')
        db.execute('DELETE FROM predictions')
        db.execute('DELETE FROM users')
        db.commit()
        
        # Verify no timestamp exists before
        before = db.execute("SELECT value FROM metadata WHERE key = 'drivers_last_refresh'").fetchone()
        assert before is None
        
        success, message = refresh_drivers_from_api(db)
        assert success is True
        
        # Verify timestamp exists after
        after = db.execute("SELECT value FROM metadata WHERE key = 'drivers_last_refresh'").fetchone()
        assert after is not None
        assert after['value'] is not None
        # Should be a valid ISO timestamp
        datetime.fromisoformat(after['value'])
