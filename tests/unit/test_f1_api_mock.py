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
    """Test race_manager functions with mocked API (RI-010)."""

    @responses.activate
    def test_ri_010_mock_for_jolpi_ergast_endpoints(self):
        """RI-010: Mock configured for Jolpi/Ergast API endpoints.
        
        Given the race_manager module uses F1 API
        When _fetch_podium is called with mocked API
        Then the correct podium data is returned
        """
        import os
        os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'
        
        # Mock the API response
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
                                {"position": "1", "Driver": {"code": "VER", "givenName": "Max", "familyName": "Verstappen", "displayName": "Max Verstappen"}, "Constructor": {"name": "Red Bull Racing"}},
                                {"position": "2", "Driver": {"code": "NOR", "givenName": "Lando", "familyName": "Norris", "displayName": "Lando Norris"}, "Constructor": {"name": "McLaren"}},
                                {"position": "3", "Driver": {"code": "LEC", "givenName": "Charles", "familyName": "Leclerc", "displayName": "Charles Leclerc"}, "Constructor": {"name": "Ferrari"}},
                            ]
                        }]
                    }
                }
            },
            status=200
        )
        
        # Import after setting env and registering mock
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
        
        # Need to reimport to pick up new env
        import importlib
        import race_manager as rm
        importlib.reload(rm)
        
        # Test fetching podium
        podium = rm._fetch_podium(2026, 1)
        
        assert podium is not None
        assert podium['p1']['driver_code'] == 'VER'
        assert podium['p2']['driver_code'] == 'NOR'
        assert podium['p3']['driver_code'] == 'LEC'

    @responses.activate
    def test_ri_010_mock_returns_none_for_no_results(self):
        """RI-010 variant: Mock returns None when race has no results yet.
        
        Given the API is mocked to return empty results
        When _fetch_podium is called
        Then None is returned (indicating race not completed)
        """
        import os
        os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'
        
        responses.add(
            responses.GET,
            'https://api.jolpi.ca/ergast/f1/2026/99/results.json',
            json={"MRData": {"RaceTable": {"Races": []}}},
            status=200
        )
        
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))
        import importlib
        import race_manager as rm
        importlib.reload(rm)
        
        podium = rm._fetch_podium(2026, 99)
        
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
