"""Unit tests for F1 API mock fixture (F1-INFRA-3: Create embedded F1 API mock)."""

import pytest
import responses


class TestF1ApiMockFixture:
    """Test cases for the F1 API mock fixture using responses library (CJ-001)."""

    @responses.activate
    def test_cj_001_responses_mock_configured(self):
        """CJ-001: responses mock is configured and working.
        
        Given the responses library is installed
        When HTTP requests are made to mocked endpoints
        Then the mock responses are returned instead of real API calls
        """
        # Register a mock response
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/1/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "1",
                            "raceName": "Bahrain Grand Prix",
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
        
        # Make the request
        import requests
        resp = requests.get('https://api.jolpi.ca/ergast/f1/2026/1/results.json')
        
        # Verify mock response
        assert resp.status_code == 200
        data = resp.json()
        assert len(data['MRData']['RaceTable']['Races']) == 1
        assert data['MRData']['RaceTable']['Races'][0]['raceName'] == 'Bahrain Grand Prix'
        assert len(data['MRData']['RaceTable']['Races'][0]['Results']) == 3

    @responses.activate
    def test_cj_001_mock_can_be_customized(self):
        """CJ-001 variant: Mock responses can be customized per test.
        
        Given the responses mock is active
        When different tests need different API responses
        Then each test can register its own mock responses
        """
        # Custom mock for this specific test
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/5/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "5",
                            "raceName": "Miami Grand Prix",
                            "Results": []
                        }]
                    }
                }
            },
            status=200
        )
        
        import requests
        resp = requests.get('https://api.jolpi.ca/ergast/f1/2026/5/results.json')
        
        assert resp.status_code == 200
        data = resp.json()
        assert data['MRData']['RaceTable']['Races'][0]['raceName'] == 'Miami Grand Prix'


class TestRaceManagerWithMock:
    """Test race_manager functions with a mocked OpenF1 client (RI-010).

    F1-01: race_manager now resolves podiums through src/openf1.py rather
    than building Jolpi/Ergast URLs itself, so these mock openf1.get_podium
    directly instead of registering HTTP fixtures.
    """

    def test_ri_010_fetch_podium_resolves_driver_ids(self):
        """RI-010: _fetch_podium resolves an OpenF1 podium to DB driver ids.

        Given openf1.get_podium returns a full podium
        And the drivers table has matching car numbers
        When _fetch_podium is called
        Then the DB driver ids are returned
        """
        import os
        import sys
        import sqlite3
        from unittest.mock import patch

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
        import race_manager as rm

        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row
        db.execute('''
            CREATE TABLE drivers (
                id INTEGER PRIMARY KEY, driver_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL, number INTEGER NOT NULL, code TEXT, nationality TEXT
            )
        ''')
        db.executemany(
            'INSERT INTO drivers (id, driver_id, name, number, code) VALUES (?, ?, ?, ?, ?)',
            [(1, 'ver', 'Max Verstappen', 1, 'VER'),
             (2, 'nor', 'Lando Norris', 4, 'NOR'),
             (3, 'lec', 'Charles Leclerc', 16, 'LEC')],
        )
        db.commit()

        openf1_podium = {
            'p1': {'position': 1, 'driver_number': 1, 'driver_name': 'Max Verstappen'},
            'p2': {'position': 2, 'driver_number': 4, 'driver_name': 'Lando Norris'},
            'p3': {'position': 3, 'driver_number': 16, 'driver_name': 'Charles Leclerc'},
        }

        with patch('race_manager.openf1.get_podium', return_value=openf1_podium):
            podium = rm._fetch_podium(db, session_key=123)

        assert podium is not None
        assert podium['p1']['driver_id'] == 1
        assert podium['p2']['driver_id'] == 2
        assert podium['p3']['driver_id'] == 3

    def test_ri_010_mock_returns_none_for_no_results(self):
        """RI-010 variant: None when race has no results yet.

        Given openf1.get_podium returns None (race not complete)
        When _fetch_podium is called
        Then None is returned (indicating race not completed)
        """
        import os
        import sys
        import sqlite3
        from unittest.mock import patch

        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
        import race_manager as rm

        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row

        with patch('race_manager.openf1.get_podium', return_value=None):
            podium = rm._fetch_podium(db, session_key=999)

        assert podium is None


class TestResponsesLibraryUsage:
    """Additional tests demonstrating responses library features."""

    @responses.activate
    def test_multiple_endpoints_can_be_mocked(self):
        """Multiple API endpoints can be mocked simultaneously."""
        # Mock driver endpoint
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/drivers.json',
            json={
                "MRData": {
                    "DriverTable": {
                        "Drivers": [
                            {"driverId": "verstappen", "code": "VER", "givenName": "Max", "familyName": "Verstappen"},
                            {"driverId": "norris", "code": "NOR", "givenName": "Lando", "familyName": "Norris"},
                        ]
                    }
                }
            },
            status=200
        )
        
        # Mock results endpoint
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/1/results.json',
            json={
                "MRData": {
                    "RaceTable": {
                        "Races": [{
                            "season": "2026",
                            "round": "1",
                            "Results": []
                        }]
                    }
                }
            },
            status=200
        )
        
        import requests
        
        drivers_resp = requests.get('https://api.jolpi.ca/ergast/f1/2026/drivers.json')
        results_resp = requests.get('https://api.jolpi.ca/ergast/f1/2026/1/results.json')
        
        assert drivers_resp.status_code == 200
        assert results_resp.status_code == 200
        
        drivers_data = drivers_resp.json()
        assert len(drivers_data['MRData']['DriverTable']['Drivers']) == 2

    @responses.activate
    def test_mock_can_simulate_error(self):
        """Mock can simulate API errors."""
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/1/results.json',
            json={"error": "Service unavailable"},
            status=503
        )
        
        import requests
        resp = requests.get('https://api.jolpi.ca/ergast/f1/2026/1/results.json')
        
        assert resp.status_code == 503
