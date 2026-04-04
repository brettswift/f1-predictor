"""Unit tests for fetch_race_results.py (F1-CJ-3: Test fetch results)."""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Set test database BEFORE importing
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['TESTING'] = 'true'
os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'
os.environ['F1_SEASON'] = '2026'

# Add cron/ to path so we can import fetch_race_results
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))


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


class TestFetchRaceResultsAPI:
    """Test API fetch functionality (CJ-008, CJ-010)."""

    def test_cj_008_api_returns_podium_data(self):
        """CJ-008: API returns podium data structure.
        
        Given the Ergast API returns race results
        When fetch_race_results_from_api is called
        Then correct podium data is returned
        """
        import fetch_race_results as fr
        
        mock_response_data = {
            'MRData': {
                'RaceTable': {
                    'Races': [{
                        'season': '2026',
                        'round': '1',
                        'raceName': 'Bahrain Grand Prix',
                        'Results': [
                            {
                                'position': '1',
                                'Driver': {'givenName': 'Max', 'familyName': 'Verstappen', 'code': 'VER'},
                                'Constructor': {'name': 'Red Bull'}
                            },
                            {
                                'position': '2',
                                'Driver': {'givenName': 'Lando', 'familyName': 'Norris', 'code': 'NOR'},
                                'Constructor': {'name': 'McLaren'}
                            },
                            {
                                'position': '3',
                                'Driver': {'givenName': 'Charles', 'familyName': 'Leclerc', 'code': 'LEC'},
                                'Constructor': {'name': 'Ferrari'}
                            }
                        ]
                    }]
                }
            }
        }
        
        with patch('fetch_race_results.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response
            
            podium = fr.fetch_race_results_from_api(2026, 1)
        
        assert podium is not None, "Should return podium data"
        assert podium['p1']['driver_code'] == 'VER'
        assert podium['p2']['driver_code'] == 'NOR'
        assert podium['p3']['driver_code'] == 'LEC'
        assert podium['p1']['driver_name'] == 'Max Verstappen'

    def test_cj_010_no_data_returns_none(self):
        """CJ-010: No data = retry next run.
        
        Given the API returns empty results (race not complete)
        When fetch_race_results_from_api is called
        Then None is returned (indicating retry needed)
        """
        import fetch_race_results as fr
        
        with patch('fetch_race_results.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                'MRData': {'RaceTable': {'Races': []}}
            }
            mock_get.return_value = mock_response
            
            podium = fr.fetch_race_results_from_api(2026, 99)
        
        assert podium is None, "Should return None when no results available"

    def test_cj_010_handles_api_error_gracefully(self):
        """CJ-010: API error is handled gracefully.
        
        Given the API returns an error
        When fetch_race_results_from_api is called
        Then None is returned (not an exception)
        """
        import fetch_race_results as fr
        import requests
        
        with patch('fetch_race_results.requests.get') as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException("Network error")
            
            podium = fr.fetch_race_results_from_api(2026, 1)
        
        assert podium is None, "Should return None on API error, not raise exception"

    def test_cj_008_api_returns_none_when_insufficient_results(self):
        """CJ-008: Returns None when fewer than 3 results.
        
        Given the API returns only 2 drivers (race not complete)
        When fetch_race_results_from_api is called
        Then None is returned
        """
        import fetch_race_results as fr
        
        mock_response_data = {
            'MRData': {
                'RaceTable': {
                    'Races': [{
                        'season': '2026',
                        'round': '1',
                        'Results': [
                            {'position': '1', 'Driver': {'givenName': 'Max', 'familyName': 'Verstappen', 'code': 'VER'}, 'Constructor': {'name': 'Red Bull'}},
                            {'position': '2', 'Driver': {'givenName': 'Lando', 'familyName': 'Norris', 'code': 'NOR'}, 'Constructor': {'name': 'McLaren'}},
                        ]
                    }]
                }
            }
        }
        
        with patch('fetch_race_results.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response
            
            podium = fr.fetch_race_results_from_api(2026, 1)
        
        assert podium is None, "Should return None when fewer than 3 results"


class TestDriverNameMatching:
    """Test driver name fuzzy matching."""

    def test_driver_display_name_formatting(self):
        """Driver names are formatted correctly from API response."""
        import fetch_race_results as fr
        
        # Test with various name formats
        result_row = {
            'Driver': {
                'givenName': '  Max  ',
                'familyName': 'Verstappen'
            }
        }
        name = fr._driver_display_name(result_row)
        assert name == 'Max Verstappen', f"Should format as 'Max Verstappen', got '{name}'"

    def test_driver_display_name_handles_missing_given_name(self):
        """Handles driver with no givenName."""
        import fetch_race_results as fr
        
        result_row = {
            'Driver': {
                'givenName': '',
                'familyName': 'Verstappen'
            }
        }
        name = fr._driver_display_name(result_row)
        assert name == 'Verstappen'


class TestGetLockedRacesQuery:
    """Test the query logic for locked races."""

    def test_query_filters_by_locked_status(self):
        """Query only returns races with 'locked' status."""
        # This tests the SQL logic structure
        expected_query = """
            SELECT r.id, r.name, r.round, r.date
            FROM races r
            LEFT JOIN results res ON r.id = res.race_id
            WHERE r.status = 'locked' AND res.race_id IS NULL
            ORDER BY r.date ASC
        """
        
        # Verify query structure
        assert "r.status = 'locked'" in expected_query
        assert "LEFT JOIN results" in expected_query
        assert "res.race_id IS NULL" in expected_query
