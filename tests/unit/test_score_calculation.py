"""Unit tests for score calculation (F1-RI-2: Test score calculation accuracy)."""

import pytest
import os
import sys

# Set test database BEFORE importing race_manager
os.environ['DATABASE_PATH'] = ':memory:'
os.environ['F1_API_URL'] = 'https://api.jolpi.ca/ergast/f1'
os.environ['F1_SEASON'] = '2026'

# Add cron/ to path so we can import race_manager
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cron'))

# Import after setting env vars
import race_manager as rm


class TestScoreCalculation:
    """Test cases for score calculation accuracy (F1-RI-2)."""

    def test_ri_003_perfect_prediction_scores_20_points(self):
        """RI-003: Perfect prediction = 20 points (10+6+4).
        
        Given a user predicts all three podium positions correctly
        When scores are calculated
        Then the user should receive 20 points (10 for P1 + 6 for P2 + 4 for P3)
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        
        score = rm._calculate_score(pred, res)
        
        assert score == 20, f"Perfect prediction should score 20 points, got {score}"

    def test_ri_004_partial_match_scores_11_points(self):
        """RI-004: Partial match = 11 points (10+0+1).
        
        Given a user predicts P1 correctly
        And predicts P2 incorrectly (driver not on podium)
        And predicts P3 with a driver on podium but in wrong position
        When scores are calculated
        Then the user should receive 11 points (10 + 0 + 1)
        """
        # Prediction: P1=A, P2=B, P3=C
        # Results: P1=A, P2=X (not on podium), P3=B (on podium wrong position)
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 1, 'p2_driver_id': 4, 'p3_driver_id': 2}
        
        score = rm._calculate_score(pred, res)
        
        assert score == 11, f"Partial match should score 11 points (10+0+1), got {score}"

    def test_ri_005_all_wrong_scores_0_points(self):
        """RI-005: All wrong = 0 points.
        
        Given a user predicts drivers that are not on the podium at all
        When scores are calculated
        Then the user should receive 0 points
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 4, 'p2_driver_id': 5, 'p3_driver_id': 6}
        
        score = rm._calculate_score(pred, res)
        
        assert score == 0, f"All wrong should score 0 points, got {score}"

    def test_ri_006_driver_on_podium_wrong_position_scores_1_point(self):
        """RI-006: Driver in podium wrong position = 1 point each.
        
        Given a user predicts a driver correctly for being on the podium
        But places them in the wrong position
        When scores are calculated
        Then the user should receive 1 point for that driver
        
        Scenario: P1 and P2 correct position, P3 on podium but wrong position
        Expected: 10 + 6 + 0 + 1 = 17 points
        """
        # Prediction: P1=A, P2=B, P3=C
        # Results: P1=A (correct), P2=B (correct), P3=X where X is C but C is in wrong position
        # Actually: Results: P1=A, P2=B, P3=C is perfect (20 points)
        # Let's use a different scenario:
        # Prediction: P1=A, P2=B, P3=C
        # Results: P1=A, P2=C, P3=B - all on podium but P2 and P3 swapped
        # P1 correct: +10
        # P2 wrong (C is on podium but in P3 position): +0, then +1 for C being on podium wrong
        # P3 wrong (B is on podium but in P2 position): +0, then +1 for B being on podium wrong
        # Total: 10 + 0 + 1 + 0 + 1 = 12
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 1, 'p2_driver_id': 3, 'p3_driver_id': 2}
        
        score = rm._calculate_score(pred, res)
        
        # All 3 drivers on podium but P2 and P3 swapped
        # P1 exact: +10
        # P2 not exact: +0 for position, +1 for being on podium
        # P3 not exact: +0 for position, +1 for being on podium
        # Total: 10 + 1 + 1 = 12
        assert score == 12, f"Driver on podium wrong position should score 12 (10+1+1), got {score}"

    def test_single_driver_on_podium_wrong_position(self):
        """Additional: Only P1 correct, P2 and P3 wrong but on podium in wrong positions.
        
        Scenario: P1 correct, P2 and P3 on podium but both in wrong positions.
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 1, 'p2_driver_id': 3, 'p3_driver_id': 2}
        
        score = rm._calculate_score(pred, res)
        
        # 10 (P1) + 0 + 1 (P2 on podium wrong) + 0 + 1 (P3 on podium wrong) = 12
        assert score == 12

    def test_p2_and_p3_correct_position(self):
        """Additional: P1 wrong, P2 and P3 correct position.
        
        P2 correct (+6) + P3 correct (+4) = 10
        P1 driver not on podium, so no extra.
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 4, 'p2_driver_id': 2, 'p3_driver_id': 3}
        
        score = rm._calculate_score(pred, res)
        
        assert score == 10, f"P2 and P3 correct should score 10 points, got {score}"

    def test_all_three_on_podium_wrong_positions(self):
        """Additional: All predicted drivers on podium but all in wrong positions.
        
        No exact matches, but all 3 drivers are on podium.
        Each gets 1 point for being on podium (wrong position).
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 3, 'p2_driver_id': 1, 'p3_driver_id': 2}
        
        score = rm._calculate_score(pred, res)
        
        # All wrong position, all on podium: 1 + 1 + 1 = 3
        assert score == 3, f"All on podium wrong positions should score 3 points, got {score}"

    def test_p1_and_p2_swapped_only(self):
        """Additional: P1 and P2 correct drivers but swapped positions.
        
        P1 predicted 1, got 2: 2 is on podium but wrong position (+1)
        P2 predicted 2, got 1: 1 is on podium but wrong position (+1)
        P3 predicted 3, got 3: exact match (+4)
        """
        pred = {'p1_driver_id': 1, 'p2_driver_id': 2, 'p3_driver_id': 3}
        res = {'p1_driver_id': 2, 'p2_driver_id': 1, 'p3_driver_id': 3}
        
        score = rm._calculate_score(pred, res)
        
        # P3 exact: +4
        # P1 wrong: +0, +1 (on podium wrong)
        # P2 wrong: +0, +1 (on podium wrong)
        # Total: 4 + 1 + 1 = 6
        assert score == 6, f"P1/P2 swapped with P3 correct should score 6, got {score}"
